# gologin_api.py
import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

import requests
from flask import Flask, jsonify, request

# --- Basic Setup ---
# Determine the absolute path of the script's directory.
APP_ROOT = Path(__file__).parent.resolve()

# --- Logging Configuration ---
LOGS_DIR = APP_ROOT / 'logs'
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / 'gologin_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- Application Configuration ---
class Config:
    DATA_DIR = APP_ROOT / 'data'
    ACCOUNTS_FILE = DATA_DIR / 'gologin_accounts.json'
    PROFILE_STATS_DIR = DATA_DIR / 'profile_stats'
    GOLOGIN_API_URL = "https://api.gologin.com/browser/v2"


class DataManager:
    """Handles all file-based data storage for the API."""

    @staticmethod
    def read_json_file(file_path: Path) -> Any:
        if not file_path.exists():
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error reading or parsing JSON from {file_path}: {e}")
            return None

    @staticmethod
    def write_json_file(file_path: Path, data: Any):
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            logger.info(f"Successfully wrote data to {file_path}")
        except IOError as e:
            logger.error(f"Error writing JSON to {file_path}: {e}")

    # --- GoLogin Account Management ---
    @classmethod
    def get_all_accounts(cls) -> Dict[str, Any]:
        accounts = cls.read_json_file(Config.ACCOUNTS_FILE)
        accounts = accounts if isinstance(accounts, dict) else {}
        # Backfill adoption keys for older records
        changed = False
        now = datetime.now()
        unlock_timeout = timedelta(minutes=5)

        for account_name, a in accounts.items():
            if "adopted" not in a:
                a["adopted"] = False; changed = True
            if "adopted_by" not in a:
                a["adopted_by"] = None; changed = True
            if "adopted_at" not in a:
                a["adopted_at"] = None; changed = True

            # Auto-unlock stale adoptions
            if a.get("adopted") and a.get("adopted_at"):
                try:
                    adopted_time = datetime.fromisoformat(a["adopted_at"])
                    if now - adopted_time > unlock_timeout:
                        logger.warning(
                            f"Auto-unlocking account '{account_name}' due to timeout. "
                            f"Adopted by '{a.get('adopted_by')}' at {a.get('adopted_at')}."
                        )
                        a["adopted"] = False
                        a["adopted_by"] = None
                        a["adopted_at"] = None
                        changed = True
                except (ValueError, TypeError):
                    # If adopted_at is not a valid ISO format string, log it but don't crash.
                    logger.error(f"Could not parse 'adopted_at' timestamp for account '{account_name}': {a.get('adopted_at')}")

        if changed:
            cls.write_json_file(Config.ACCOUNTS_FILE, accounts)
        return accounts
    @classmethod
    def set_adoption(cls, account_name: str, adopted: bool, adopted_by: Optional[str]):
        accounts = cls.get_all_accounts()
        if account_name not in accounts:
            return False, "Account not found"
        acct = accounts[account_name]
        acct["adopted"] = adopted
        acct["adopted_by"] = adopted_by if adopted else None
        acct["adopted_at"] = datetime.now().isoformat() if adopted else None
        cls.write_json_file(Config.ACCOUNTS_FILE, accounts)
        return True, None
        
    @classmethod
    def save_account(cls, account_name: str, token: str) -> bool:
        accounts = cls.get_all_accounts()
        accounts[account_name] = {
            "token": token,
            "profiles": [],
            # NEW adoption fields
            "adopted": False,
            "adopted_by": None,
            "adopted_at": None,
        }
        cls.write_json_file(Config.ACCOUNTS_FILE, accounts)
        return True

    @classmethod
    def update_account_profiles(cls, account_name: str, profile_ids: List[str]):
        accounts = cls.get_all_accounts()
        if account_name in accounts:
            accounts[account_name]["profiles"] = profile_ids
            cls.write_json_file(Config.ACCOUNTS_FILE, accounts)
            logger.info(f"Updated profile list for account '{account_name}'.")
        else:
            logger.warning(f"Attempted to update profiles for non-existent account '{account_name}'.")

    # --- Profile Stats Management ---
    @classmethod
    def get_stats_file_path(cls, account_name: str) -> Path:
        return Config.PROFILE_STATS_DIR / f"{account_name}_stats.json"

    @classmethod
    def save_profile_stats(cls, account_name: str, stats_data: Dict[str, Any]):
        stats_file = cls.get_stats_file_path(account_name)
        cls.write_json_file(stats_file, stats_data)

    @classmethod
    def get_profile_stats(cls, account_name: str) -> Optional[Dict[str, Any]]:
        stats_file = cls.get_stats_file_path(account_name)
        return cls.read_json_file(stats_file)


# --- Flask App Initialization ---
app = Flask(__name__)
data_manager = DataManager()

def ensure_directories():
    """Creates all necessary data directories on startup."""
    try:
        Config.DATA_DIR.mkdir(exist_ok=True)
        Config.PROFILE_STATS_DIR.mkdir(exist_ok=True)
        LOGS_DIR.mkdir(exist_ok=True)
        logger.info("Verified that all necessary directories exist.")
    except Exception as e:
        logger.critical(f"Could not create necessary directories: {e}", exc_info=True)
        raise

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not Found", "message": "The requested endpoint does not exist."}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal Server Error: {error}", exc_info=True)
    return jsonify({"error": "Internal Server Error", "message": "An unexpected error occurred on the server."}), 500


# --- API Endpoints ---

@app.route("/accounts", methods=["GET"])
def list_accounts():
    """Lists the names of all configured GoLogin accounts."""
    accounts = data_manager.get_all_accounts()
    return jsonify({"accounts": list(accounts.keys())})

# --- NEW ENDPOINT TO GET A SINGLE ACCOUNT'S DETAILS ---
@app.route("/accounts/<string:account_name>", methods=["GET"])
def get_account_details(account_name):
    """Returns all details for a specific account, including the token."""
    accounts = data_manager.get_all_accounts()
    account_info = accounts.get(account_name)
   
    if not account_info:
        return jsonify({"error": f"Account '{account_name}' not found."}), 404
       
    # The account_info already contains {"token": "...", "profiles": [...]}.
    return jsonify(account_info)
# --- END OF NEW ENDPOINT ---

@app.route("/accounts/<string:account_name>/adopt", methods=["POST"])
def adopt_account(account_name):
    """
    Claim or release an account for a specific VPS.
    Body: {"action": "claim"|"release", "adopted_by": "vps-123"}
    - claim: if already adopted by another VPS -> 409
    - release: sets adopted=false (adopted_by optional)
    """
    data = request.get_json(silent=True) or {}
    action = data.get("action")
    vps_id = data.get("adopted_by")

    if action not in ("claim", "release"):
        return jsonify({"error": "action must be 'claim' or 'release'"}), 400

    accounts = data_manager.get_all_accounts()
    acct = accounts.get(account_name)
    if not acct:
        return jsonify({"error": f"Account '{account_name}' not found."}), 404

    if action == "claim":
        if not vps_id:
            return jsonify({"error": "adopted_by is required for claim"}), 400
        if acct.get("adopted") and acct.get("adopted_by") != vps_id:
            # already claimed by someone else
            return jsonify({
                "status": "conflict",
                "message": f"Account already adopted by {acct.get('adopted_by')}"
            }), 409
        ok, err = data_manager.set_adoption(account_name, True, vps_id)
        if not ok:
            return jsonify({"error": err or "Failed to claim"}), 500
        return jsonify({"status": "claimed", "account_name": account_name, "adopted_by": vps_id})

    # release
    ok, err = data_manager.set_adoption(account_name, False, None)
    if not ok:
        return jsonify({"error": err or "Failed to release"}), 500
    return jsonify({"status": "released", "account_name": account_name})

@app.route("/accounts", methods=["POST"])
def add_account():
    """Adds a new GoLogin account with its API token."""
    data = request.get_json()
    if not data or "account_name" not in data or "token" not in data:
        return jsonify({"error": "Request must include 'account_name' and 'token'"}), 400
    
    account_name = data["account_name"]
    token = data["token"]
    
    data_manager.save_account(account_name, token)
    return jsonify({"status": "success", "message": f"Account '{account_name}' saved."}), 201

@app.route("/accounts/<string:account_name>/fetch-profiles", methods=["POST"])
def fetch_and_save_profiles(account_name):
    """Fetches the profile list for an account from GoLogin and saves it."""
    accounts = data_manager.get_all_accounts()
    account_info = accounts.get(account_name)
    
    if not account_info:
        return jsonify({"error": f"Account '{account_name}' not found."}), 404
        
    token = account_info.get("token")
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        logger.info(f"Fetching profiles from GoLogin API for account '{account_name}'...")
        response = requests.get(Config.GOLOGIN_API_URL, headers=headers, timeout=15)
        response.raise_for_status()
        
        response_data = response.json()
        
        if not isinstance(response_data, dict) or 'profiles' not in response_data:
            logger.error(f"GoLogin API returned unexpected data format: {response_data}")
            return jsonify({"error": "Received unexpected data format from GoLogin API."}), 500
        
        profiles_list = response_data.get('profiles', [])
        
        if not isinstance(profiles_list, list):
            logger.error(f"The 'profiles' key did not contain a list. Found: {type(profiles_list)}")
            return jsonify({"error": "Received unexpected data format inside 'profiles' key."}), 500

        profile_ids = [p.get('id') for p in profiles_list if 'id' in p]
        
        data_manager.update_account_profiles(account_name, profile_ids)
        
        return jsonify({
            "status": "success",
            "account_name": account_name,
            "profiles_fetched": len(profile_ids),
            "profile_ids": profile_ids
        })
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            return jsonify({"error": "Unauthorized. The API token for this account is invalid."}), 401
        return jsonify({"error": f"HTTP Error from GoLogin API: {e.response.status_code}"}), e.response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Network error connecting to GoLogin API: {e}"}), 503

@app.route("/accounts/<string:account_name>/stats", methods=["GET"])
def get_stats(account_name):
    """Retrieves the saved stats for a specific account."""
    stats = data_manager.get_profile_stats(account_name)
    if stats is None:
        return jsonify({"error": f"No stats found for account '{account_name}'."}), 404
    return jsonify(stats)

@app.route("/accounts/<string:account_name>/stats", methods=["POST"])
def save_stats(account_name):
    """Saves the profile stats for a specific account."""
    stats_data = request.get_json()
    if not stats_data:
        return jsonify({"error": "Request must include stats data in JSON body."}), 400
    
    stats_data["last_updated"] = datetime.now().isoformat()
    data_manager.save_profile_stats(account_name, stats_data)
    
    return jsonify({"status": "success", "message": f"Stats saved for account '{account_name}'."})

@app.route("/accounts/<string:account_name>/check-limit", methods=["GET"])
def check_account_limit(account_name):
    """Checks if a GoLogin account has reached its free API request limit."""
    logger.info(f"Checking API limit for account: '{account_name}'...")
    
    accounts = data_manager.get_all_accounts()
    account_info = accounts.get(account_name)
    
    if not account_info:
        return jsonify({"error": f"Account '{account_name}' not found."}), 404
        
    token = account_info.get("token")
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(Config.GOLOGIN_API_URL, headers=headers, timeout=15)
        
        if "You have reached your free API requests limit" in response.text:
            logger.warning(f"API limit reached for account '{account_name}'.")
            return jsonify({
                "account_name": account_name,
                "status": "limit_exceeded",
                "limit_reached": True
            })
        
        response.raise_for_status()
        
        logger.info(f"API limit is OK for account '{account_name}'.")
        return jsonify({
            "account_name": account_name,
            "status": "ok",
            "limit_reached": False
        })
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            return jsonify({"error": "Unauthorized. The API token for this account is invalid."}), 401
        return jsonify({"error": f"HTTP Error from GoLogin API: {e.response.status_code}"}), e.response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Network error connecting to GoLogin API: {e}"}), 503


# --- Main Execution ---
if __name__ == "__main__":
    ensure_directories()
    try:
        app.run(host="0.0.0.0", port=8080, debug=False)
    except Exception as e:
        logger.critical(f"Failed to start Flask server: {e}", exc_info=True)
        sys.exit(1)
