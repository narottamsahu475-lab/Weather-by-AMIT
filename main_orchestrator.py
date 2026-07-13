import asyncio
import aiohttp
import re
import time
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION & GLOBAL STATES ---
FOUR_SIM_API_KEY = "6788f58a862c093c2167b7e57d62e122"
SPYEYE_API_KEY = "35GUU4PBADS"
BASE_URL = "https://spyeyeloots.online"

GAME_MAP = {
    "567slot": {"api": "567slots", "ui": "567Slots"},
    "MBMBet": {"api": "mbmbet", "ui": "MBMBet"},
    "Bingo": {"api": "bingo101", "ui": "Bingo"},
    "789Jackpot": {"api": "789jackpots", "ui": "789Jackpot"},
    "SpinCrush": {"api": "spincrush", "ui": "Spin Crush"},
    "HiRummy": {"api": "hirummy", "ui": "HiRummy"},
    "Maha": {"api": "mahagames", "ui": "Maha"},
    "YonoVip": {"api": "yonovip", "ui": "YonoVip"},
    "789Jackpots": {"api": "789jackpots", "ui": "789Jackpots"},
    "MaxRummy": {"api": "maxrummy", "ui": "MaxRummy"},
    "YonoGames": {"api": "yonogames", "ui": "YonoGames"},
    "INDRummy": {"api": "indrummy", "ui": "INDrummy"},
    "YonoSlots": {"api": "yonoslots", "ui": "YonoSlots"}
}

live_stats = {
    "total_targeted": 0,
    "total_thread_count": 0,
    "active_threads": 0,
    "success_otps": 0,
    "already_registered": 0,
    "cancelled_orders": 0,
    "total_secured": 0,
    "system_status": "Ready to Start",
    "pipeline_running": False,
    "progress": 0,
    "eta": "---",
    "success_records": [],
    "recent_activity": [],
    "game_analytics": {},
    "registration_summary": {},
    "activity_timeline": [],
    "health_check": {
        "internet": "Connected",
        "4sim": "Connected",
        "SPYEYE": "Connected"
    },
    "realtime_active_threads": 0,
    "realtime_already_logs": 0,
    "cancel_failed_logs": 0,
    "cancel_failed_numbers_list": [],
    "spyeye_balance": "₹0",
    "foursim_balance": "00"
}

stats_lock = asyncio.Lock()
buy_lock = asyncio.Lock()
stop_event = asyncio.Event()

logged_cancels = set()
active_already_tasks = set()

success_buy_count = 0
input_total_accounts = 0
active_task_counter = 0
global_service_id = "1929" 


# ==========================================
#          REUSABLE SPYEYE API CLIENT    
# ==========================================
class SpyEyeClient:
    def __init__(self, base_url: str, access_code: str):
        self.base_url = base_url
        self.access_code = access_code

    async def send_otp(self, session: aiohttp.ClientSession, app_name: str, number: str) -> dict:
        url = f"{self.base_url}/yono?app={app_name}&action=sendotp&number={number}&accesscode={self.access_code}"
        async with session.get(url, ssl=False, timeout=12) as resp:
            return await resp.json()

    async def verify_otp(self, session: aiohttp.ClientSession, app_name: str, request_id: str, otp: str) -> dict:
        url = f"{self.base_url}/yono?app={app_name}&action=verify&requestid={request_id}&otp={otp}&accesscode={self.access_code}"
        async with session.get(url, ssl=False, timeout=15) as resp:
            return await resp.json()

    async def cancel_request(self, session: aiohttp.ClientSession, app_name: str, request_id: str) -> dict:
        url = f"{self.base_url}/yono?app={app_name}&action=cancel&requestid={request_id}&accesscode={self.access_code}"
        async with session.get(url, ssl=False, timeout=10) as resp:
            return await resp.json()

    async def resend_otp(self, session: aiohttp.ClientSession, app_name: str, request_id: str) -> dict:
        url = f"{self.base_url}/yono?app={app_name}&action=resendotp&requestid={request_id}&accesscode={self.access_code}"
        async with session.get(url, ssl=False, timeout=10) as resp:
            return await resp.json()

    async def check_status(self, session: aiohttp.ClientSession, app_name: str, request_id: str) -> dict:
        url = f"{self.base_url}/yono?app={app_name}&action=status&requestid={request_id}&accesscode={self.access_code}"
        async with session.get(url, ssl=False, timeout=10) as resp:
            return await resp.json()

    async def get_account_info(self, session: aiohttp.ClientSession) -> dict:
        url = f"{self.base_url}/yono-api/login?accesscode={self.access_code}"
        async with session.get(url, ssl=False, timeout=10) as resp:
            return await resp.json()

    async def get_history(self, session: aiohttp.ClientSession, date_str: str = None) -> dict:
        url = f"{self.base_url}/yono-api/history?accesscode={self.access_code}"
        if date_str:
            url += f"&date={date_str}"
        async with session.get(url, ssl=False, timeout=12) as resp:
            return await resp.json()

spyeye_client = SpyEyeClient(BASE_URL, SPYEYE_API_KEY)


# --- TIMELINE & STATUS METRIC LOGGERS ---
async def log_game_metric(game_name, status="success"):
    async with stats_lock:
        if game_name not in live_stats["game_analytics"]:
            live_stats["game_analytics"][game_name] = {"success": 0, "failed": 0, "already": 0}
        if status == "success":
            live_stats["game_analytics"][game_name]["success"] += 1
        elif status == "already":
            live_stats["game_analytics"][game_name]["already"] += 1
        else:
            live_stats["game_analytics"][game_name]["failed"] += 1

async def add_timeline_event(phone, stage):
    async with stats_lock:
        live_stats["activity_timeline"].insert(0, {
            "time": datetime.now().strftime("%H:%M:%S"),
            "phone": phone,
            "stage": stage
        })
        if len(live_stats["activity_timeline"]) > 30:
            live_stats["activity_timeline"].pop()

async def update_live_status(phone, status_text, balance_text=None, log_type="active", target_game=None, retry_idx=None, progress_val=None):
    async with stats_lock:
        for num_entry in live_stats["recent_activity"]:
            if num_entry["phone"] == phone:
                num_entry["status"] = status_text
                num_entry["log_type"] = log_type
                if balance_text is not None:
                    num_entry["balance"] = balance_text
                if target_game is not None:
                    num_entry["current_game"] = target_game
                if retry_idx is not None:
                    num_entry["retry"] = retry_idx
                if progress_val is not None:
                    num_entry["progress"] = progress_val
                break

async def fetch_smart_otp_async(txn_id, used_otps):
    url = f"https://api.4sim.st/checkSms?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                res = await response.json()
                sms_text = str(res.get("sms") or res.get("code") or "")
                if sms_text:
                    all_codes = re.findall(r'\b\d{4}\b|\b\d{6}\b', sms_text)
                    new_codes = [c for c in all_codes if c not in used_otps]
                    if new_codes: 
                        return new_codes[-1]
    except: 
        pass
    return None

async def terminate_4sim_order_async(txn_id, otp_received, phone, force_cancel=False):
    global logged_cancels
    async with stats_lock:
        if txn_id in logged_cancels:
            return "Already Handled"

    if force_cancel or not otp_received:
        url = f"https://api.4sim.st/cancelNumber?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
        max_retries = 6
        is_cancel = True
    else:
        url = f"https://api.4sim.st/finishOrder?apikey={FOUR_SIM_API_KEY}&id={txn_id}"
        max_retries = 1
        is_cancel = False

    final_status = "Failed Completely"
    
    async with aiohttp.ClientSession() as session:
        for attempt in range(1, max_retries + 1):
            try:
                async with session.get(url, timeout=10) as response:
                    status_code = response.status
                    raw_text = (await response.text()).strip()

                    if status_code in [200, 400]:
                        if any(x in raw_text.lower() for x in ["error", "fail", "please wait"]):
                            if "please wait" in raw_text.lower():
                                match = re.search(r"[\d.]+", raw_text)
                                wait_seconds = int(float(match.group())) if match else 30
                                await update_live_status(phone, "Released Asset Hold", log_type="already")
                                await asyncio.sleep(wait_seconds + 5)
                                continue
                            final_status = f"API_Err: {raw_text[:25]}"
                            break
                        final_status = "Released" if is_cancel else "Finished Successfully"
                        break
                    else:
                        if "already cancelled" in raw_text.lower() or "not found" in raw_text.lower():
                            final_status = "Released"
                            break
                        if "please wait" in raw_text.lower():
                            match = re.search(r"[\d.]+", raw_text)
                            wait_seconds = int(float(match.group())) if match else 30
                            await update_live_status(phone, "Terminating Assets", log_type="already")
                            await asyncio.sleep(wait_seconds + 5)
                            continue
                        final_status = f"HTTP_{status_code}: {raw_text[:20]}"
            except:
                pass
            await asyncio.sleep(3)
                
    if final_status in ["Released", "Finished Successfully", "Already Cancelled"]:
        async with stats_lock:
            logged_cancels.add(txn_id)
            if is_cancel:
                live_stats["cancelled_orders"] += 1
    else:
        async with stats_lock:
            live_stats["cancel_failed_logs"] += 1
            if phone not in live_stats["cancel_failed_numbers_list"]:
                live_stats["cancel_failed_numbers_list"].append({
                    "phone": phone,
                    "txn_id": txn_id,
                    "reason": final_status,
                    "time": datetime.now().strftime("%H:%M:%S")
                })
    return final_status

async def handle_already_number(phone, txn_id, otp_flag):
    current_task = asyncio.current_task()
    active_already_tasks.add(current_task)
    
    async with stats_lock:
        live_stats["realtime_already_logs"] = len(active_already_tasks)
        
    try:
        remaining_seconds = 140
        await update_live_status(phone, "Release Hold", log_type="already")
        await add_timeline_event(phone, "Moved to Delayed Queue (Already Reg)")
        
        while remaining_seconds > 0:
            await asyncio.sleep(2)
            remaining_seconds -= 2
            
        await terminate_4sim_order_async(txn_id, otp_flag, phone, force_cancel=True)
        await update_live_status(phone, "Cool-off Completed", log_type="cancel")
    except:
        pass
    finally:
        active_already_tasks.discard(current_task)
        async with stats_lock:
            live_stats["realtime_already_logs"] = len(active_already_tasks)

async def run_game_step_async(phone, txn_id, game_key, used_otps, is_sub_game=False):
    if game_key not in GAME_MAP:
        return "failed", False

    # UPDATED: Timeout 35s se 45s karne ke liye max_attempts 9 kiya (9 * 5s = 45s)
    max_attempts = 9 if is_sub_game else 26
    send_attempt = 1
    last_known_request_id = None
    
    api_name = GAME_MAP[game_key]["api"]
    ui_name = GAME_MAP[game_key]["ui"]
    
    async with aiohttp.ClientSession() as session:
        while send_attempt <= 6:
            try:
                await update_live_status(phone, "Sending OTP...", target_game=ui_name, retry_idx=send_attempt, progress_val=15)
                
                v_res = await spyeye_client.send_otp(session, api_name, phone)
                
                if v_res.get("requestid") or v_res.get("task_id"):
                    last_known_request_id = v_res.get("requestid") or v_res.get("task_id")
                    
                error_msg = str(v_res.get("msg") or v_res.get("message") or "").lower()
                
                if v_res.get("success") is not True and v_res.get("status") != "success":
                    if "already" in error_msg or "555" in error_msg:
                        await update_live_status(phone, "Already Reg", progress_val=0)
                        await log_game_metric(ui_name, "already")
                        return "already", False

                    display_err = v_res.get("msg") or v_res.get("message") or "API Hold Error"
                    await update_live_status(phone, f"API Hold: {display_err[:22]}", progress_val=5)
                    
                    if "rate limit" in error_msg:
                        await asyncio.sleep(10)
                    else:
                        await asyncio.sleep(5)
                    send_attempt += 1
                    continue
                    
                if v_res.get("success") is True or v_res.get("status") == "success":
                    request_id = v_res.get("requestid") or v_res.get("task_id")
                    last_known_request_id = request_id
                    await update_live_status(phone, "Waiting OTP...", progress_val=40)
                    
                    otp_found_flag = False
                    for attempt in range(1, max_attempts + 1):
                        await asyncio.sleep(5)
                        otp = await fetch_smart_otp_async(txn_id, used_otps)
                        if otp:
                            otp_found_flag = True
                            await update_live_status(phone, "Submitting OTP...", progress_val=75)
                            
                            verify_res = await spyeye_client.verify_otp(session, api_name, request_id, otp)
                            v_msg = str(verify_res.get("msg") or "").lower()
                            
                            if verify_res.get("success") is True or verify_res.get("status") == "success":
                                bal_val = verify_res.get("balance") or verify_res.get("data", {}).get("account_balance", 0)
                                bal = str(int(float(bal_val))) + " INR"
                                
                                await update_live_status(phone, "SUCCESS", balance_text=bal, progress_val=100)
                                used_otps.add(otp)
                                
                                async with stats_lock:
                                    if ui_name not in live_stats["registration_summary"]:
                                        live_stats["registration_summary"][ui_name] = 0
                                    live_stats["registration_summary"][ui_name] += 1
                                    
                                await log_game_metric(ui_name, "success")
                                await add_timeline_event(phone, f"Success Registered -> {ui_name}")
                                return True, True
                                
                            elif "already registered" in v_msg or "555" in v_msg:
                                await update_live_status(phone, "Already Reg", progress_val=0)
                                used_otps.add(otp)
                                await log_game_metric(ui_name, "already")
                                return "already", True
                                
                            else:
                                await update_live_status(phone, "Wrong/Expired OTP", progress_val=90)
                                used_otps.add(otp)
                                await log_game_metric(ui_name, "failed")
                                return False, True
                                
                    # UPDATED: Agar 45s tak OTP na aaye, toh task ko SPYEYE se cancel karna hai
                    if not otp_found_flag:
                        await update_live_status(phone, "Canceling SPYEYE...", progress_val=10)
                        try:
                            cancel_res = await spyeye_client.cancel_request(session, api_name, request_id)
                            if cancel_res.get("success") is True:
                                await update_live_status(phone, "SPYEYE Refunded", progress_val=0)
                        except: 
                            pass
                    else:
                        await update_live_status(phone, "Timeout", progress_val=0)
                        
                    await log_game_metric(ui_name, "failed")
                    return "timeout", otp_found_flag

            except Exception:
                await update_live_status(phone, "Err: Connect Dropped", progress_val=5)
                send_attempt += 1
                await asyncio.sleep(4)
                
        if last_known_request_id:
            await update_live_status(phone, "Max Fail: Canceling...", progress_val=5)
            try:
                await spyeye_client.cancel_request(session, api_name, last_known_request_id)
            except: 
                pass

        await update_live_status(phone, "Failed Sending", progress_val=0)
        await log_game_metric(ui_name, "failed")
        return "failed", False

async def process_single_registration():
    global success_buy_count, active_task_counter, input_total_accounts, global_service_id
    
    if stop_event.is_set() or success_buy_count >= input_total_accounts: 
        return
    
    phone, txn_id = None, None
    used_otps = set()
    otp_received_anywhere = False
    
    success_chains_count = 0
    already_chains_count = 0
    failed_or_timeout_count = 0
    
    registered_games_list = []
    last_known_balance = "₹0"
    
    async with buy_lock:
        if stop_event.is_set() or success_buy_count >= input_total_accounts: 
            return
        async with stats_lock: 
            live_stats["system_status"] = "Securing Stock..."
        
        buy_url = f"https://api.4sim.st/buyNumber?apikey={FOUR_SIM_API_KEY}&id={global_service_id}&country=22"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(buy_url, timeout=12) as response:
                    buy_res = await response.json()
            phone = str(buy_res.get("number", ""))[-10:]
            txn_id = buy_res.get("tid") or buy_res.get("id")
            if not phone: 
                return
            success_buy_count += 1
            active_task_counter += 1
        except: 
            return

    async with stats_lock:
        live_stats["total_secured"] = success_buy_count
        live_stats["realtime_active_threads"] += 1
        live_stats["recent_activity"].insert(0, {
            "phone": phone,
            "current_game": "567slot",
            "status": "Initializing...",
            "balance": "₹0",
            "retry": 1,
            "progress": 0,
            "thread_color": "🟢",
            "log_type": "active"
        })
    
    await add_timeline_event(phone, "Acquired New Number from 4Sim")

    main_res, m_otp_flag = await run_game_step_async(phone, txn_id, "567slot", used_otps, is_sub_game=False)
    if m_otp_flag: 
        otp_received_anywhere = True
    
    if main_res == "already":
        async with stats_lock: 
            live_stats["already_registered"] += 1
            live_stats["realtime_active_threads"] = max(0, live_stats["realtime_active_threads"] - 1)
        asyncio.create_task(handle_already_number(phone, txn_id, otp_received_anywhere))
        return
        
    elif main_res in ["timeout", "failed", False]:
        async with stats_lock:
            live_stats["realtime_active_threads"] = max(0, live_stats["realtime_active_threads"] - 1)
        await update_live_status(phone, "Released", log_type="cancel")
        await terminate_4sim_order_async(txn_id, otp_received_anywhere, phone)
        return
        
    elif main_res is True:
        async with stats_lock: 
            live_stats["success_otps"] += 1
        success_chains_count += 1
        registered_games_list.append("567slot")
        await asyncio.sleep(2) 
        
        other_games = ["YonoGames", "YonoSlots", "SpinCrush", "789Jackpots", "MBMBet", "Bingo", "YonoVip" "HiRummy", "Maha"]
        for game in other_games:
            sub_res, s_otp_flag = await run_game_step_async(phone, txn_id, game, used_otps, is_sub_game=True)
            if s_otp_flag: 
                otp_received_anywhere = True
            
            if sub_res is True:
                success_chains_count += 1
                registered_games_list.append(game)
                async with stats_lock:
                    for item in live_stats["recent_activity"]:
                        if item["phone"] == phone and "INR" in str(item["balance"]):
                            last_known_balance = "₹" + str(item["balance"]).replace(" INR", "")
                await asyncio.sleep(2)
            elif sub_res == "already":
                already_chains_count += 1
            else:
                failed_or_timeout_count += 1

        await terminate_4sim_order_async(txn_id, otp_received_anywhere, phone)
        
        async with stats_lock:
            live_stats["success_records"].insert(0, {
                "phone": phone,
                "games": registered_games_list,
                "wallet": last_known_balance,
                "success": success_chains_count,
                "already": already_chains_count,
                "failed": failed_or_timeout_count,
                "time": datetime.now().strftime("%H:%M:%S")
            })
            live_stats["progress"] = int((success_buy_count / input_total_accounts) * 100)
            live_stats["realtime_active_threads"] = max(0, live_stats["realtime_active_threads"] - 1)
            
        await update_live_status(phone, "Released", log_type="cancel")

async def dynamic_pipeline_runner(semaphore):
    while not stop_event.is_set() and success_buy_count < input_total_accounts:
        async with semaphore:
            await process_single_registration()
        await asyncio.sleep(1)

# UPDATED: REAL-TIME POLLING BACKGROUND TASK FOR 4SIM & SPYEYE WALLET BALANCES
async def live_balances_tracker_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # 1. Fetch SPYEYE balance
                async with session.get(f"{BASE_URL}/yono-api/login?accesscode={SPYEYE_API_KEY}", ssl=False, timeout=8) as r1:
                    d1 = await r1.json()
                    if d1.get("success"):
                        async with stats_lock:
                            live_stats["spyeye_balance"] = "₹" + str(int(float(d1.get("credits", 0))))
                
                # 2. Fetch 4Sim balance
                async with session.get(f"https://api.4sim.st/getBalance?apikey={FOUR_SIM_API_KEY}", timeout=8) as r2:
                    d2 = await r2.json()
                    if d2.get("balance"):
                        async with stats_lock:
                            live_stats["foursim_balance"] = "₹" + str(int(float(d2.get("balance"))))
        except:
            pass
        await asyncio.sleep(2)

async def core_engine_orchestrator(target, threads):
    global live_stats, success_buy_count, active_task_counter, input_total_accounts, logged_cancels
    
    success_buy_count = 0
    active_task_counter = 0
    input_total_accounts = target
    
    logged_cancels.clear()
    active_already_tasks.clear()
    
    async with stats_lock:
        live_stats["total_targeted"] = target
        live_stats["total_thread_count"] = threads
        live_stats["active_threads"] = threads
        live_stats["pipeline_running"] = True
        live_stats["success_otps"] = 0
        live_stats["already_registered"] = 0
        live_stats["cancelled_orders"] = 0
        live_stats["total_secured"] = 0
        live_stats["progress"] = 0
        live_stats["eta"] = "Calculating..."
        live_stats["recent_activity"] = []
        live_stats["game_analytics"] = {} 
        live_stats["registration_summary"] = {}
        live_stats["activity_timeline"] = []
        live_stats["realtime_active_threads"] = 0
        live_stats["realtime_already_logs"] = 0
        live_stats["cancel_failed_logs"] = 0
        live_stats["cancel_failed_numbers_list"] = []
    
    semaphore = asyncio.Semaphore(threads)
    workers = [asyncio.create_task(dynamic_pipeline_runner(semaphore)) for _ in range(threads)]
    
    start_time = time.time()
    while success_buy_count < input_total_accounts and not stop_event.is_set():
        async with stats_lock:
            live_stats["active_threads"] = len([w for w in workers if not w.done()])
            live_stats["system_status"] = "Running | Active Pipeline Loop"
            
            elapsed = time.time() - start_time
            if success_buy_count > 0:
                avg_time = elapsed / success_buy_count
                rem_acc = input_total_accounts - success_buy_count
                eta_secs = int(avg_time * rem_acc)
                live_stats["eta"] = f"{eta_secs // 60}m {eta_secs % 60}s"
                
        await asyncio.sleep(1)
        
    async with stats_lock:
        live_stats["system_status"] = "Stop Received. Completing active runs..."
    
    await asyncio.gather(*workers, return_exceptions=True)
    
    while len(active_already_tasks) > 0:
        async with stats_lock:
            live_stats["system_status"] = f"Awaiting {len(active_already_tasks)} delay-cancels..."
        await asyncio.sleep(1)
        
    async with stats_lock:
        live_stats["pipeline_running"] = False
        live_stats["active_threads"] = 0
        live_stats["realtime_active_threads"] = 0
        live_stats["system_status"] = "Pipeline Finished / Idle"
        live_stats["eta"] = "---"

# --- SPYEYE UTILITY ROUTER ENDPOINTS ---
@app.get("/api/spyeye/account-info")
async def get_spyeye_account_info():
    async with aiohttp.ClientSession() as session:
        try:
            data = await spyeye_client.get_account_info(session)
            return JSONResponse(content=data)
        except Exception as e:
            return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/spyeye/history")
async def get_spyeye_history(date: str = None):
    async with aiohttp.ClientSession() as session:
        try:
            data = await spyeye_client.get_history(session, date_str=date)
            return JSONResponse(content=data)
        except Exception as e:
            return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.post("/api/start")
async def api_start(request: Request):
    global global_service_id
    if stop_event.is_set(): 
        stop_event.clear()
    data = await request.json()
    target = int(data.get('target', 10))
    threads = int(data.get('threads', 2))
    global_service_id = str(data.get('service_id', '1929')).strip()
    
    asyncio.create_task(core_engine_orchestrator(target, threads))
    return {"status": "success"}

@app.post("/api/stop")
async def api_stop():
    stop_event.set()
    return {"status": "graceful_stop_initiated"}

@app.get("/api/logs")
async def api_logs():
    return JSONResponse(content=live_stats)

# Start background balance polling loop on application startup context
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(live_balances_tracker_loop())

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    try:
        with open(BASE_DIR / "panel.html", "r", encoding="utf-8") as f: 
            return f.read()
    except:
        return "<h3>panel.html file missing inside target app space</h3>"
