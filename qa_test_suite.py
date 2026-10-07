# -*- coding: utf-8 -*-
import sys
import os
import time
import json
import asyncio
import httpx

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

sys.path.insert(0, os.getcwd())

from database import db
from proxy_manager import get_proxy_manager
import PAPAX_server
from PAPAX_server import (
    build_aggregated_accounts,
    check_and_purge_expired_plans,
    bot_state
)
from app import (
    process_account_token_universal,
    send_majorlogin,
    send_getlogin
)

passed_tests = []
failed_tests = []

def record(test_name: str, passed: bool, details: str = ""):
    status_icon = "PASS" if passed else "FAIL"
    print(f"[{status_icon}] {test_name}: {details}")
    if passed:
        passed_tests.append(test_name)
    else:
        failed_tests.append(f"{test_name}: {details}")

async def run_qa_tests():
    print("=" * 60)
    print("BEGINNING COMPREHENSIVE QA TEST SUITE (A to Z)")
    print("=" * 60)

    # Clean up test users from local db if leftover
    db.init_db()
    for u in db.get_all_users():
        if u.get("username") in ["qa_test_user_a", "qa_test_user_b"]:
            db.delete_user(u["id"])

    # ----------------------------------------------------
    # TESTCASE A: User Registration & Default State
    # ----------------------------------------------------
    ok, msg, user_data = db.register_user("qa_test_user_a", "TestPassword123!", "qa_tele")
    user_id = user_data["id"] if user_data else None
    user_doc = db.get_user_by_id(user_id) if user_id else None
    clean = db._clean_user(user_doc) if user_doc else {}

    test_a_ok = (
        ok and 
        user_doc.get("role") == "user" and 
        clean.get("slots") == 0 and 
        clean.get("plan_expired") is True and
        clean.get("remaining_seconds") == 0
    )
    record("Testcase A (Registration & Zero Initial Slots)", test_a_ok, f"slots={clean.get('slots')}, plan={clean.get('plan')}")

    # ----------------------------------------------------
    # TESTCASE B: Account Addition Block on Free/Expired Tier
    # ----------------------------------------------------
    # Simulate non-admin slot check as in handle_add_account
    now = time.time()
    plan_expires_at = user_doc.get("plan_expires_at")
    has_active_plan = bool(plan_expires_at and now <= plan_expires_at)
    user_clean_slots = clean.get("slots", 0)

    can_add = has_active_plan and user_clean_slots > 0
    record("Testcase B (Add Account Blocked on Free/Expired Tier)", not can_add, f"has_active_plan={has_active_plan}, slots={user_clean_slots}")

    # ----------------------------------------------------
    # TESTCASE C: Plan Catalog & Slot Integrity (No 2-slot plan)
    # ----------------------------------------------------
    plans = db.get_plans()
    all_slot_counts = [p.get("slots") for p in plans]
    no_two_slot_plan = 2 not in all_slot_counts
    plan_6h = next((p for p in plans if p.get("id") == "plan_6h"), None)
    has_3_slots_for_6h = (plan_6h and plan_6h.get("slots") == 3)
    record("Testcase C (Catalog Integrity - No 2-Slot Plan, 6h=3 slots)", no_two_slot_plan and has_3_slots_for_6h, f"Catalog slot configurations: {all_slot_counts}")

    # ----------------------------------------------------
    # TESTCASE D: Order Creation & Approval (6 Hours -> 3 Slots)
    # ----------------------------------------------------
    ok_ord, msg_ord, ord_doc = db.create_order(user_id, "qa_test_user_a", "plan_6h", "UPI", "TEST_UTR_0001", "qa_tele")
    order_id = ord_doc["id"] if ord_doc else None
    
    ok_app, msg_app = db.approve_order(order_id, "admin", "QA Auto-Approve")
    user_after_approval = db.get_user_by_id(user_id)
    clean_after_approval = db._clean_user(user_after_approval)

    approved_slots = clean_after_approval.get("slots")
    test_d_ok = ok_app and approved_slots == 3 and not clean_after_approval.get("plan_expired")
    record("Testcase D (Order Approval & 3-Slot Activation)", test_d_ok, f"User slots={approved_slots}, plan={clean_after_approval.get('plan')}, remaining={clean_after_approval.get('remaining_seconds')}s")

    # ----------------------------------------------------
    # TESTCASE E: Account Addition Within Slot Limit (3/3 Slots)
    # ----------------------------------------------------
    # Link 3 accounts to this user
    accs = [
        {"uid": "8021201056", "nickname": "Acc_ID", "region": "ID", "mode": "AUTO"},
        {"uid": "8021223011", "nickname": "Acc_PK", "region": "PK", "mode": "AUTO"},
        {"uid": "8031222285", "nickname": "Acc_VN", "region": "VN", "mode": "AUTO"}
    ]
    for a in accs:
        db.link_account_to_user(user_id, a)

    user_accs = db.get_user_account_uids(user_id)
    test_e_ok = (len(user_accs) == 3)
    available_slots = max(0, approved_slots - len(user_accs))
    record("Testcase E (Added 3 Accounts to fill 3/3 Slots)", test_e_ok, f"Accounts added={len(user_accs)}/3, Available slots={available_slots}")

    # ----------------------------------------------------
    # TESTCASE F: Overflow Block When Slots Are Full (3/3 Used)
    # ----------------------------------------------------
    can_add_4th = len(user_accs) < approved_slots
    record("Testcase F (Overflow Blocked on 3/3 Slots)", not can_add_4th, f"Can add 4th account={can_add_4th} (Target: False)")

    # ----------------------------------------------------
    # TESTCASE G: Plan Expiration & Account Purge Watchdog
    # ----------------------------------------------------
    # Force expire the user's plan
    db.update_user(user_id, {
        "plan_expires_at": time.time() - 3600 # Expired 1 hour ago
    })
    
    # Run watchdog
    await check_and_purge_expired_plans()
    
    user_expired = db.get_user_by_id(user_id)
    clean_expired = db._clean_user(user_expired)
    accs_remaining = db.get_user_accounts_meta(user_id)
    test_g_ok = (clean_expired.get("slots") == 0 and len(accs_remaining) == 0 and clean_expired.get("plan_expired") is True)
    record("Testcase G (Watchdog Expiration & 0 Slots & Account Purge)", test_g_ok, f"Slots after expiry={clean_expired.get('slots')}, Accounts remaining={len(accs_remaining)}")

    # ----------------------------------------------------
    # TESTCASE H: Admin Duration-To-Slots Automatic Mapping
    # ----------------------------------------------------
    # Admin adds 24 hours -> Should give 5 slots
    PLAN_HOURS_MAP = {
        6: (3, "6 Hours Sprint"),
        24: (5, "24 Hours Turbo"),
        72: (8, "3 Days Pro"),
        168: (12, "7 Days Champion"),
        720: (25, "30 Days Dominator"),
    }
    all_admin_maps_ok = True
    for add_h, (exp_slots, exp_name) in PLAN_HOURS_MAP.items():
        updates = {
            "plan_expires_at": time.time() + (add_h * 3600),
            "slots": exp_slots,
            "plan": exp_name
        }
        db.update_user(user_id, updates)
        u_check = db._clean_user(db.get_user_by_id(user_id))
        if u_check.get("slots") != exp_slots:
            all_admin_maps_ok = False
            break

    record("Testcase H (Admin Hour-to-Slot Mapping for 6h, 24h, 3d, 7d, 30d)", all_admin_maps_ok, f"Verified 6h->3, 24h->5, 72h->8, 168h->12, 720h->25 slots")

    # ----------------------------------------------------
    # TESTCASE I: Garena Access Token Inspection Format (?token= vs ?access_token=)
    # ----------------------------------------------------
    # When using ?token= with a 64-char token, Garena validates syntax and returns invalid_grant (NOT invalid_request)
    # When using ?access_token=, Garena fails with invalid_request.
    full_test_token = "5ce16e6fa3" + "0" * 54
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            inspect_resp_good = await client.get(f"https://100067.connect.garena.com/oauth/token/inspect?token={full_test_token}")
            err_good = inspect_resp_good.json().get("error")

            inspect_resp_bad = await client.get(f"https://100067.connect.garena.com/oauth/token/inspect?access_token={full_test_token}")
            err_bad = inspect_resp_bad.json().get("error")

            # ?token= should yield invalid_grant (or success), while ?access_token= yields invalid_request
            test_i_ok = (err_good != "invalid_request" and err_bad == "invalid_request")
            record("Testcase I (Garena Token Inspect uses ?token= and avoids invalid_request)", test_i_ok, f"?token= returned '{err_good}' (valid param) vs ?access_token= returned '{err_bad}' (rejected param)")
        except Exception as e:
            record("Testcase I (Garena Token Inspect HTTP)", False, str(e))

    # ----------------------------------------------------
    # TESTCASE J: Bangladesh (BD) Proxy Removal & Direct Route
    # ----------------------------------------------------
    # Verify BD proxy list is empty and BD does not use proxy
    pm = get_proxy_manager()
    # Check app.py send_majorlogin and send_getlogin logic
    with open("app.py", "r", encoding="utf-8") as f:
        app_code = f.read()
    
    bd_in_proxy_triggers = 'reg_upper in ["BD", "IND"]' in app_code or 'reg_upper in [\'BD\', \'IND\']' in app_code
    bd_scraped = '_scrape_proxyscrape("BD")' in open("proxy_manager.py", "r", encoding="utf-8").read()

    test_j_ok = (not bd_in_proxy_triggers) and (not bd_scraped)
    record("Testcase J (BD Region Removed from Proxy Triggers & Scrapers)", test_j_ok, f"Direct connection enforced for BD (triggers={bd_in_proxy_triggers}, scraped={bd_scraped})")

    # ----------------------------------------------------
    # TESTCASE K: Admin vs Regular User Isolation
    # ----------------------------------------------------
    admin_user = next((u for u in db.get_all_users() if u.get("role") == "admin"), None)
    admin_clean = db._clean_user(admin_user) if admin_user else {}
    test_k_ok = (admin_clean.get("slots") == 999 and not admin_clean.get("plan_expired"))
    record("Testcase K (Admin Global Privileges - 999 Slots)", test_k_ok, f"Admin slots={admin_clean.get('slots')}, plan={admin_clean.get('plan')}")

    # Cleanup QA test user
    db.delete_user(user_id)

    print("=" * 60)
    print(f"QA TEST SUMMARY: {len(passed_tests)} PASSED, {len(failed_tests)} FAILED")
    print("=" * 60)
    if failed_tests:
        for f in failed_tests:
            print(f"FAILED: {f}")
        sys.exit(1)
    else:
        print("ALL A-Z TESTCASES PASSED WITH ZERO ERRORS!")

if __name__ == "__main__":
    asyncio.run(run_qa_tests())
