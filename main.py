import os
import random
import re
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, error
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, TypeHandler
from telegram.constants import ChatType
from typing import Dict, List, Tuple
from pymongo import MongoClient

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "WordRushDB")
try:
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID")) 
except (TypeError, ValueError):
    ADMIN_USER_ID = 0 
    logger.warning("⚠️ ADMIN_USER_ID not set or invalid. Admin features will be disabled.")

# --- Configuration (Capped at 8 letters and using only 8-letter for hard/extreme) ---
DIFFICULTY_CONFIG = {
    'easy': {'length': 4, 'max_guesses': 30, 'base_points': 5, 'example': 'GAME'},
    'medium': {'length': 5, 'max_guesses': 30, 'base_points': 10, 'example': 'APPLE'},
    'hard': {'length': 8, 'max_guesses': 30, 'base_points': 20, 'example': 'FOOTBALL'},
    'extreme': {'length': 8, 'max_guesses': 30, 'base_points': 50, 'example': 'FOOTBALL'} 
}

# --- Word List (Using only up to 8-letter words) ---
RAW_WORDS = [
    # 4-Letter Words
    "GAME", "FOUR", "FIRE", "WORD", "PLAY", "CODE", "RUNS", "STOP", "LOOK", "CALL", "BACK", "BEST", "FAST", "SLOW", "HIGH", "LOWS", 
    "OPEN", "CLOS", "READ", "WRIT", "BOOK", "PAGE", "LINE", "JUMP", "WALK", "TALK", "QUIZ", "TEST", "RAIN", "SNOW", "SUNY", "COLD", 
    "HEAT", "WIND", "MIST", "DUST", "ROCK", "SAND", "SOIL", "GRAS", "TREE", "LEAF", "ROOT", "STEM", "SEED", "GROW", "CROP", "FARM", 
    "BLUE", "PINK", "GOLD", "IRON", "COAL", "MINE", "RICH", "POOR", "WAVE", "FISH", "BIRD", "LION", "BEAR", "WOLF", "DEER", "DUCK",
    "FROG", "CRAB", "STAR", "MOON", "PLAN", "TIME", "HOUR", "DATE", "YEAR", "MIND", "SOUL", "LIFE", "DEAD", "BORN", "BABY", "KIDS",
    "TEAM", "GOAL", "WINS", "LOSS", "ZONE", "ROSE", "WIND", "SHIP", "BOAT", "CITY", "TOWN", "LAND", "KING", "WISH", "HOPE", "LOVE",
    # 5-Letter Words
    "APPLE", "HEART", "WATER", "TABLE", "PLANT", "TIGER", "EAGLE", "SNAKE", "WHALE", "ZEBRA", "SOUND", "MUSIC", "RADIO", "VOICE", 
    "BEACH", "OCEAN", "RIVER", "LAKE", "FIELD", "CABLE", "WIRED", "PHONE", "EMAIL", "SCARY", "HAPPY", "FUNNY", "SADLY", "ANGER", 
    "BRAVE", "CHAIR", "BENCH", "CUPPY", "GLASS", "PLATE", "FORKS", "KNIFE", "SPOON", "SUGAR", "SALTZ", "BREAD", "CHEES", "MEATS", 
    "SHARK", "CLOUD", "STORM", "LIGHT", "NIGHT", "CLOCK", "WATCH", "SMART", "SWEET", "SHARP", "ROUND", "GREEN", "WHITE", "BLACK",
    "BROWN", "HOUSE", "PLACE", "WORLD", "SPACE", "TRAIN", "PLANE", "MOTOR", "WHEEL", "BRUSH", "PAINT", "PAPER", "WRITE", "PRINT",
    "DREAM", "SLEEP", "SMILE", "LAUGH", "STONE", "BRICK", "STARS", "FLAME", "LIGHT", "MATCH", "FIGHT", "CROWN", "MAGIC", "FRUIT",
    # 8-Letter Words
    "FOOTBALL", "COMPUTER", "KEYBOARD", "MEMORIZE", "INTERNET", "PROGRAMS", "SOFTWARE", "HARDWARE", "DATABASE", "ALGORISM", 
    "SECURITY", "PASSWORD", "TELEGRAM", "BUSINESS", "FINANCES", "MARKETIN", "ADVERTSZ", "STRATEGY", "MANUFACT", "PRODUCTS", 
    "CHAMPION", "CREATIVE", "DESIGNER", "DOCUMENT", "ENGINEER", "FEEDBACK", "FESTIVAL", "FORECAST", "FRIENDLY", "GARDENER",
    "HOSPITAL", "IDENTITY", "INDUSTRY", "LANGUAGE", "MOUNTAIN", "NAVIGATE", "PERSONAL", "PLATFORM", "POSITION", "QUESTION",
    "REACTION", "STRENGTH", "UNIVERSE", "VALUABLE", "PRACTICE"
]

WORDS_BY_LENGTH: Dict[int, List[str]] = {}
for word in RAW_WORDS:
    cleaned_word = "".join(filter(str.isalpha, word.upper())) 
    length = len(cleaned_word)
    if length <= 8 and length in [c['length'] for c in DIFFICULTY_CONFIG.values()]: 
         WORDS_BY_LENGTH.setdefault(length, []).append(cleaned_word)
         
# --- MongoDB Manager Class (Unchanged from your last version) ---
class MongoDBManager:
    """Handles all interactions with MongoDB, now with time-based leaderboards and clone support."""
    def __init__(self, mongo_url: str, db_name: str):
        if not mongo_url:
            raise ValueError("MONGO_URL not provided.")
        
        self.client = MongoClient(mongo_url, serverSelectionTimeoutMS=10000) 
        self.db = self.client[db_name]
        self.leaderboard_collection = self.db['leaderboard']
        self.games_collection = self.db['active_games']
        self.chats_collection = self.db['known_chats'] 
        self.members_collection = self.db['chat_members']
        
        self.leaderboard_collection.create_index("user_id", unique=True)

        # Drop old single chat_id unique indexes if they exist
        try:
            self.games_collection.drop_index("chat_id_1")
        except Exception:
            pass
        try:
            self.chats_collection.drop_index("chat_id_1")
        except Exception:
            pass

        # Create new unique compound indexes partitioned by bot_username
        self.games_collection.create_index([("chat_id", 1), ("bot_username", 1)], unique=True)
        self.chats_collection.create_index([("chat_id", 1), ("bot_username", 1)], unique=True)
        self.members_collection.create_index([("chat_id", 1), ("user_id", 1), ("bot_username", 1)], unique=True)
        logger.info("✅ MongoDB connection and indexing successful.")

    def _get_reset_check_query(self, user_id: int, period: str) -> dict:
        """Determines if the points/wins for a period need a reset."""
        now = datetime.now(timezone.utc)
        
        if period == 'daily':
            reset_after = now - timedelta(days=1)
        elif period == 'weekly':
            reset_after = now - timedelta(weeks=1)
        elif period == 'monthly':
            reset_after = now - timedelta(days=30)
        else: # Global
            return {'$set': {}}

        # $lt checks if the last win was BEFORE the reset threshold
        return {
            '$inc': {f'points_{period}': 0, f'wins_{period}': 0}, # Dummy $inc to allow $set
            '$set': {
                f'points_{period}': 0, 
                f'wins_{period}': 0,
            }
        }, {f'last_win_date_{period}': {'$lt': reset_after}}


    def update_leaderboard(self, user_id: int, username: str, points_to_add: int):
        now = datetime.now(timezone.utc)
        update_global = {
            '$inc': {'points_global': points_to_add, 'wins_global': 1},
            '$set': {'username': username}
        }
        
        # 1. Update Global stats
        self.leaderboard_collection.update_one(
            {'user_id': user_id},
            update_global,
            upsert=True
        )
        
        # 2. Update Time-based stats
        periods = ['daily', 'weekly', 'monthly']
        for period in periods:
            update_op, reset_query = self._get_reset_check_query(user_id, period)
            
            # 2a. Check if reset is needed and perform reset if true
            # We use $inc: 0 and $set to conditionally set the points/wins to 0 
            # if the last win date is too old.
            
            reset_result = self.leaderboard_collection.update_one(
                {'user_id': user_id, f'last_win_date_{period}': {'$lt': now - timedelta(days=1) if period == 'daily' else now - timedelta(weeks=1) if period == 'weekly' else now - timedelta(days=30)}},
                {'$set': {f'points_{period}': 0, f'wins_{period}': 0}}
            )

            # 2b. Now, increment the period-specific points/wins and update the win date
            self.leaderboard_collection.update_one(
                {'user_id': user_id},
                {
                    '$inc': {f'points_{period}': points_to_add, f'wins_{period}': 1},
                    '$set': {f'last_win_date_{period}': now}
                },
                upsert=True
            )


    def get_leaderboard_data(self, period: str, limit=10) -> List[Tuple[str, int, int]]:
        """Retrieves leaderboard data for a specific period (daily, weekly, monthly, global)."""
        points_key = f'points_{period}'
        wins_key = f'wins_{period}'
        
        # Query: Find all entries, sort by points for the given period
        data = list(self.leaderboard_collection.find().sort(points_key, -1).limit(limit))
        
        result = []
        for doc in data:
            points = doc.get(points_key, 0)
            wins = doc.get(wins_key, 0)
            
            # Ensure we only show users who have actually played in this period (points > 0)
            if points > 0 or period == 'global':
                 result.append((doc.get('username'), points, wins))
                 
        # Re-sort to ensure integrity
        result.sort(key=lambda x: x[1], reverse=True)
        return result[:limit]

    def get_game_state(self, chat_id: int, bot_username: str) -> Dict | None:
        return self.games_collection.find_one({'chat_id': chat_id, 'bot_username': bot_username.lower()})

    def save_game_state(self, chat_id: int, state: Dict, bot_username: str):
        state_to_save = {'chat_id': chat_id, 'bot_username': bot_username.lower(), **state}
        self.games_collection.replace_one(
            {'chat_id': chat_id, 'bot_username': bot_username.lower()},
            state_to_save, 
            upsert=True
        )

    def delete_game_state(self, chat_id: int, bot_username: str):
        self.games_collection.delete_one({'chat_id': chat_id, 'bot_username': bot_username.lower()})

    def add_chat(self, chat_id: int, chat_type: str, date: float, bot_username: str):
        self.chats_collection.update_one(
            {'chat_id': chat_id, 'bot_username': bot_username.lower()},
            {'$set': {'chat_type': chat_type, 'last_active': date}},
            upsert=True
        )

    def get_all_chats(self, bot_username: str = None) -> List[dict]:
        if bot_username:
            return list(self.chats_collection.find({'bot_username': bot_username.lower()}))
        return list(self.chats_collection.find())

    def save_chat_member(self, chat_id: int, user_id: int, bot_username: str):
        if not user_id or user_id <= 0:
            return
        self.members_collection.update_one(
            {
                'chat_id': chat_id,
                'user_id': user_id,
                'bot_username': bot_username.lower()
            },
            {
                '$set': {
                    'chat_id': chat_id,
                    'user_id': user_id,
                    'bot_username': bot_username.lower(),
                    'last_seen': datetime.now(timezone.utc)
                }
            },
            upsert=True
        )

    def get_chat_members(self, chat_id: int, bot_username: str) -> List[int]:
        docs = self.members_collection.find({
            'chat_id': chat_id,
            'bot_username': bot_username.lower()
        })
        return [doc['user_id'] for doc in docs]

    # --- Clone Management DB Methods ---
    def save_clone(self, user_id: int, username: str, token: str, bot_username: str):
        self.db['cloned_bots'].update_one(
            {'user_id': user_id},
            {
                '$set': {
                    'username': username,
                    'token': token,
                    'bot_username': bot_username.lower(),
                    'is_active': True,
                    'created_at': datetime.now(timezone.utc)
                }
            },
            upsert=True
        )

    def get_clone_by_owner(self, user_id: int) -> dict | None:
        return self.db['cloned_bots'].find_one({'user_id': user_id})

    def get_clone_by_username(self, bot_username: str) -> dict | None:
        return self.db['cloned_bots'].find_one({'bot_username': bot_username.lower()})

    def delete_clone(self, user_id: int):
        self.db['cloned_bots'].delete_one({'user_id': user_id})

    def get_all_clones(self) -> List[dict]:
        return list(self.db['cloned_bots'].find())

    def update_clone_links(self, user_id: int, custom_channel: str = None, custom_group: str = None):
        update_fields = {}
        if custom_channel is not None:
            update_fields['custom_channel'] = custom_channel
        if custom_group is not None:
            update_fields['custom_group'] = custom_group
        if update_fields:
            self.db['cloned_bots'].update_one({'user_id': user_id}, {'$set': update_fields})

# --- Initialize MongoDB Manager ---
mongo_manager = None
try:
    if MONGO_URL:
        mongo_manager = MongoDBManager(MONGO_URL, MONGO_DB_NAME)
    else:
        logger.error("❌ MONGO_URL not set. Running without database features.")
except Exception as e:
    logger.error(f"❌ FATAL: Could not connect to MongoDB. Error: {e}")
    mongo_manager = None 

# --- Clone Manager Class ---
class CloneManager:
    """Manages the lifecycle of cloned bots concurrently in the same asyncio event loop."""
    def __init__(self, db_manager):
        self.db = db_manager
        self.clones: Dict[str, Application] = {}  # token -> Application
        self.clones_by_username: Dict[str, Application] = {}  # bot_username -> Application

    async def start_clone(self, token: str, owner_id: int) -> Tuple[bool, str]:
        token = token.strip()
        if not token:
            return False, "Token cannot be empty."

        # 1. Validate token with Telegram
        try:
            from telegram import Bot
            temp_bot = Bot(token)
            me = await temp_bot.get_me()
            bot_username = me.username.lower()
        except Exception as e:
            logger.error(f"Failed to validate token {token[:10]}...: {e}")
            return False, f"Invalid token or Telegram connection error: {e}"

        # If the owner already has a clone running, stop it first to prevent memory leak and cleanly replace it!
        if self.db:
            existing = self.db.get_clone_by_owner(owner_id)
            if existing:
                existing_token = existing.get("token")
                if existing_token:
                    await self.stop_clone(existing_token)

        # Also stop by token directly if token is already in memory
        if token in self.clones:
            await self.stop_clone(token)

        # 2. Create Application
        try:
            builder = Application.builder().token(token)
            app = builder.build()

            # Register handlers to the clone app
            register_all_handlers(app)

            # Initialize, start, and start polling
            await app.initialize()
            await app.start()
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

            self.clones[token] = app
            self.clones_by_username[bot_username] = app
            logger.info(f"Successfully started cloned bot: @{bot_username}")
            return True, bot_username
        except Exception as e:
            logger.error(f"Error starting clone @{bot_username}: {e}")
            return False, str(e)

    async def stop_clone(self, token: str) -> bool:
        token = token.strip()
        if token in self.clones:
            app = self.clones[token]
            bot_username = None
            for username, cloned_app in list(self.clones_by_username.items()):
                if cloned_app == app:
                    bot_username = username
                    break

            try:
                if app.updater and app.updater.running:
                    await app.updater.stop()
                await app.stop()
                await app.shutdown()
            except Exception as e:
                logger.error(f"Error stopping clone {bot_username or token[:10]}: {e}")

            if token in self.clones:
                del self.clones[token]
            if bot_username and bot_username in self.clones_by_username:
                del self.clones_by_username[bot_username]
            return True
        return False

    async def start_all(self):
        if not self.db:
            return
        clones_in_db = self.db.get_all_clones()
        logger.info(f"Starting {len(clones_in_db)} cloned bots from database...")
        for clone in clones_in_db:
            if clone.get("is_active", True):
                token = clone["token"]
                owner_id = clone["user_id"]
                # Start clone in background so one failure doesn't block the rest
                asyncio.create_task(self.start_clone(token, owner_id))

    async def stop_all(self):
        logger.info("Stopping all cloned bots...")
        tokens = list(self.clones.keys())
        for token in tokens:
            await self.stop_clone(token)

# --- Initialize Clone Manager ---
clone_manager = None
if mongo_manager is not None:
    clone_manager = CloneManager(mongo_manager)

# --- Core Game Logic Functions ---
# (get_feedback and calculate_points remain unchanged)

def get_feedback(secret_word: str, guess: str) -> str:
    """Generates the Wordle-style color-coded feedback (🟩, 🟨, 🟥)."""
    length = len(secret_word)
    feedback = ['🟥'] * length 
    remaining_letters = {}
    
    for letter in secret_word:
        remaining_letters[letter] = remaining_letters.get(letter, 0) + 1

    # First pass: Green (Correct position)
    for i in range(length):
        if i < len(guess) and guess[i] == secret_word[i]:
            feedback[i] = '🟩'
            remaining_letters[guess[i]] -= 1

    # Second pass: Yellow (Correct letter, wrong position)
    for i in range(length):
        if feedback[i] == '🟥' and i < len(guess):
            letter = guess[i]
            if letter in remaining_letters and remaining_letters[letter] > 0:
                feedback[i] = '🟨'
                remaining_letters[letter] -= 1
    
    return "".join(feedback)

def calculate_points(difficulty: str, guesses: int) -> int:
    """Calculates points based on difficulty and efficiency."""
    config = DIFFICULTY_CONFIG[difficulty]
    base = config['base_points']
    # Higher bonus for fewer guesses
    bonus = max(0, 10 - (guesses - 1) * 2) 
    return base + bonus

async def start_new_game_logic(chat_id: int, difficulty: str, bot_username: str) -> Tuple[bool, str]:
    if not mongo_manager: return False, "❌ *Database Error*. Game cannot be started without database access."
    
    difficulty = difficulty.lower()
    if difficulty not in DIFFICULTY_CONFIG:
        difficulty = 'medium'
        
    config = DIFFICULTY_CONFIG[difficulty]
    length = config['length']
    word_list = WORDS_BY_LENGTH.get(length)
    
    if not word_list:
        return False, f"❌ *Error*: No secret words found for **{difficulty}** ({length} letters). Contact admin."
    
    # Select a secret word from the list
    secret_word = random.choice(word_list)
    
    initial_state = {
        'word': secret_word,
        'difficulty': difficulty,
        'guesses_made': 0,
        'max_guesses': config['max_guesses'],
        'guess_history': [],
        'guessed_words': [] # NEW: To track unique words guessed
    }
    mongo_manager.save_game_state(chat_id, initial_state, bot_username)
    
    return True, (
        f"**✨ New Word Challenge!**\n"
        f"-------------------------------------\n"
        f"🎯 Difficulty: **{difficulty.capitalize()}**\n"
        f"📜 Word Length: **{length} letters** (Example: `{config['example']}`)\n"
        f"➡️ *Send your {length}-letter guess directly to the chat!*"
    )

async def process_guess_logic(chat_id: int, guess: str, bot_username: str) -> Tuple[str, bool, str, int, List[str]]:
    """Processes a user's guess and returns feedback, win status, and points."""
    if not mongo_manager: return "", False, "Database Error.", 0, []

    game = mongo_manager.get_game_state(chat_id, bot_username)
    if not game:
        return "", False, "No active game.", 0, []
    
    secret_word = game['word']
    # Guess is already cleaned/uppercase by the MessageHandler filters
    guess_clean = guess.upper()

    config = DIFFICULTY_CONFIG[game['difficulty']]
    length = config['length']
    
    # 1. Validation for length (Should match game length)
    if len(guess_clean) != length:
        # User message for incorrect length
        return "", False, f"❌ **`{guess.upper()}`** *must be exactly* **{length}** *letters long*.", 0, game.get('guess_history', [])
    
    # 2. NEW: Check if word has already been guessed
    if guess_clean in game.get('guessed_words', []):
        # User message for duplicate guess
        return "", False, f"❌ **`{guess.upper()}`** *already guessed! Try a new word*.", 0, game.get('guess_history', [])

    
    game['guesses_made'] += 1
    game['guessed_words'].append(guess_clean) # Add the valid guess to the list
    
    # 3. Generate Feedback and update history
    feedback_str = get_feedback(secret_word, guess_clean)
    # Storing in the required format: Blocks - WORD
    game['guess_history'].append(f" `{feedback_str}` - **{guess_clean}**") 
    
    # 4. Check for Win
    if guess_clean == secret_word:
        guesses = game['guesses_made']
        points = calculate_points(game['difficulty'], guesses)
        mongo_manager.delete_game_state(chat_id, bot_username)
        return feedback_str, True, "WIN", points, game['guess_history']

    # 5. Check for Loss
    remaining = game['max_guesses'] - game['guesses_made']
    
    if remaining <= 0:
        game_word_for_loss = game['word']
        mongo_manager.delete_game_state(chat_id, bot_username)
        # For loss, we return the secret word as status
        return feedback_str, False, f"LOSS_WORD:{game_word_for_loss}", 0, game['guess_history']
    
    # Status for ongoing game
    mongo_manager.save_game_state(chat_id, game, bot_username)
    return feedback_str, False, f"Guesses left: **{remaining}**", 0, game['guess_history']

# --- Telegram UI & Handler Functions (All Unchanged) ---

async def is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    # Any user is considered "admin" in a private chat for commands like /end and /difficulty
    if update.effective_chat.type == ChatType.PRIVATE:
        return True
    try:
        member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
        return member.status in ['administrator', 'creator']
    except Exception:
        return False

# --- Keyboard Functions (Unchanged) ---

async def get_bot_keyboard_links(bot_username: str) -> Tuple[str, str]:
    """Returns (updates_channel_url, report_support_url) for the bot."""
    channel_url = "https://t.me/narzob"
    group_url = "https://t.me/Onlymrabhi01"
    if mongo_manager:
        clone = mongo_manager.get_clone_by_username(bot_username)
        if clone:
            channel_url = clone.get("custom_channel") or channel_url
            group_url = clone.get("custom_group") or group_url
    return channel_url, group_url

def get_start_keyboard(bot_username: str, channel_url: str, group_url: str):
    keyboard = [
        [InlineKeyboardButton("❓ Help & Info", callback_data="show_help_menu")],
        [
            InlineKeyboardButton("💬 Report Bugs", url=group_url),
            InlineKeyboardButton("📢 Updates Channel", url=channel_url)
        ],
        [
            InlineKeyboardButton("👥 Clone Bot", callback_data="clone_menu"),
            InlineKeyboardButton("⚙️ Manage Clone", callback_data="manage_menu")
        ],
        [InlineKeyboardButton("➕ Add Bot to Group", url=f"https://t.me/{bot_username}?startgroup=true")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_help_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("📝 How to Play", callback_data="show_how_to_play")],
        [InlineKeyboardButton("📘 Commands List", callback_data="show_commands")],
        [InlineKeyboardButton("🏆 Leaderboard", callback_data="show_leaderboard_menu")],
        [InlineKeyboardButton("🏠 Back to Start", callback_data="back_to_start")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_play_again_keyboard():
    keyboard = [
        [InlineKeyboardButton("🎯 Start New Game", callback_data="new_game_menu")] 
    ]
    return InlineKeyboardMarkup(keyboard)

def get_new_game_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("⭐ Easy (4 letters)", callback_data="start_easy"),
            InlineKeyboardButton("🌟 Medium (5 letters)", callback_data="start_medium")
        ],
        [
            InlineKeyboardButton("🔥 Hard (8 letters)", callback_data="start_hard"),
            InlineKeyboardButton("💎 Extreme (8 letters, High Pts)", callback_data="start_extreme")
        ],
        [InlineKeyboardButton("🏠 Back to Start", callback_data="back_to_start")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_leaderboard_menu_keyboard(): 
    keyboard = [
        [
            InlineKeyboardButton("☀️ Daily", callback_data="show_leaderboard_daily"),
            InlineKeyboardButton("📅 Weekly", callback_data="show_leaderboard_weekly"),
        ],
        [
            InlineKeyboardButton("🗓️ Monthly", callback_data="show_leaderboard_monthly"),
            InlineKeyboardButton("🌎 Global", callback_data="show_leaderboard_global"),
        ],
        [InlineKeyboardButton("🔙 Back to Help", callback_data="show_help_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- Leaderboard Utility Function (Unchanged) ---

async def display_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE, period: str):
    """Fetches and displays the leaderboard for the given period."""
    if not mongo_manager:
        await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)("❌ *Database Error*. Cannot fetch leaderboard.")
        return

    data = mongo_manager.get_leaderboard_data(period=period, limit=10)
    
    title = period.capitalize() if period != 'global' else 'Global'
    
    if not data:
        message = f"🏆 **{title} Leaderboard**\n\n*No scores recorded for this period yet.*"
    else:
        message = f"🏆 **{title} Leaderboard** (Top 10)\n"
        message += "-------------------------------------\n"
        for i, (username, points, wins) in enumerate(data):
            rank_style = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"**{i+1}.**"
            name = f"@{username}" if username else f"User ID `{data[i][0]}`"
            message += f"{rank_style} {name} - **`{points}`** pts ({wins} wins)\n"
            
    # Send as a new message if it's a command, or edit if it's a callback
    if update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=get_leaderboard_menu_keyboard(), parse_mode='Markdown')
    else:
        await update.message.reply_text(message, reply_markup=get_leaderboard_menu_keyboard(), parse_mode='Markdown')

# --- Command Handlers (All Unchanged) ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bot_username = context.bot.username.lower()
    chat_id = update.effective_chat.id
    if mongo_manager and update.effective_message:
        mongo_manager.add_chat(chat_id, update.effective_chat.type.name, update.effective_message.date.timestamp(), bot_username)

    channel_url, group_url = await get_bot_keyboard_links(bot_username)
    
    # Stylish Start Message
    await update.message.reply_text(
        f"👋 *Hello! I'm* **@{context.bot.username}** 🤖\n"
        f"-------------------------------------\n"
        f"The **Ultimate Word Challenge** on Telegram!\n\n"
        f"📜 **Goal:** *Guess the secret word using hints (🟩/🟨/🟥).*\n"
        f"🏆 **Compete:** *Win to earn points and climb the Global Leaderboard!* 🌐\n\n"
        f"👉 Tap **/new** or the button below to start your challenge!\n"
        f"-------------------------------------",
        reply_markup=get_start_keyboard(context.bot.username, channel_url, group_url),
        parse_mode='Markdown'
    )

async def new_game_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    bot_username = context.bot.username.lower()
    if mongo_manager and update.effective_message:
        mongo_manager.add_chat(chat_id, update.effective_chat.type.name, update.effective_message.date.timestamp(), bot_username)

    difficulty = context.args[0].lower() if context.args else 'medium'
    
    if mongo_manager and mongo_manager.get_game_state(chat_id, bot_username):
        await update.message.reply_text("⏳ *A game is already active*. Use **/end** to stop it first.")
        return

    success, message = await start_new_game_logic(chat_id, difficulty, bot_username)
    await update.message.reply_text(message, parse_mode='Markdown')

async def end_game_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    bot_username = context.bot.username.lower()
    
    if not mongo_manager or not mongo_manager.get_game_state(chat_id, bot_username):
        await update.message.reply_text("❌ *No game is currently running to end*.")
        return
        
    if not await is_group_admin(update, context):
        await update.message.reply_text("🚨 *Admin Check Failed*. You must be an **Admin** to force-end the game.", parse_mode='Markdown')
        return

    game_state = mongo_manager.get_game_state(chat_id, bot_username)
    word = game_state.get('word', 'UNKNOWN')
    mongo_manager.delete_game_state(chat_id, bot_username)
    
    await update.message.reply_text(
        f"🛑 **Game Ended!**\n"
        f"*The secret word was:* **`{word}`**.", 
        parse_mode='Markdown'
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows the current game status and guess history."""
    chat_id = update.effective_chat.id
    bot_username = context.bot.username.lower()
    if not mongo_manager:
        await update.message.reply_text("❌ *Database Error*. Cannot fetch game status.")
        return

    game_state = mongo_manager.get_game_state(chat_id, bot_username)
    if not game_state:
        await update.message.reply_text("🎯 *No active game*. Use **/new** to start a challenge!")
        return
    
    guess_history = game_state.get('guess_history', [])
    
    if not guess_history:
        history_display = "*No guesses made yet!*"
    else:
        history_display = "\n".join(guess_history)

    remaining = game_state['max_guesses'] - game_state['guesses_made']
    
    reply_text = (
        f"**📊 Current Word Challenge Status**\n"
        f"-------------------------------------\n"
        f"Difficulty: **{game_state['difficulty'].capitalize()}**\n"
        f"Word Length: **{len(game_state['word'])} letters**\n"
        f"Guesses: **`{game_state['guesses_made']}`** / **`{game_state['max_guesses']}`**\n"
        f"Remaining: **`{remaining}`**\n\n"
        f"📜 **Guess History:**\n"
        f"{history_display}"
    )
    
    await update.message.reply_text(reply_text, parse_mode='Markdown')

async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows the leaderboard menu or the global leaderboard directly."""
    bot_username = context.bot.username.lower()
    if mongo_manager and update.effective_message:
        mongo_manager.add_chat(update.effective_chat.id, update.effective_chat.type.name, update.effective_message.date.timestamp(), bot_username)

    # If arguments are provided (e.g., /leaderboard daily), show that specific one
    if context.args and context.args[0].lower() in ['daily', 'weekly', 'monthly', 'global']:
        period = context.args[0].lower()
        await display_leaderboard(update, context, period)
        return

    # Otherwise, show the leaderboard menu
    message = "🏆 **Global Leaderboard**\n\n*Choose a period below to view the rankings!*"
    await update.message.reply_text(message, reply_markup=get_leaderboard_menu_keyboard(), parse_mode='Markdown')

async def difficulty_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows available difficulty levels and their settings."""
    chat_id = update.effective_chat.id

    if not await is_group_admin(update, context):
        await update.message.reply_text("🚨 *Admin Check Failed*. You must be an **Admin** to view or change settings.", parse_mode='Markdown')
        return

    message = "**⚙️ Word Challenge Difficulty Settings**\n"
    message += "-------------------------------------\n"
    
    for level, config in DIFFICULTY_CONFIG.items():
        message += f"**{level.capitalize()}**:\n"
        message += f"   - Word Length: **{config['length']}** letters\n"
        message += f"   - Max Guesses: **{config['max_guesses']}**\n"
        message += f"   - Base Points: **{config['base_points']}**\n"
        message += f"   - Example: `{config['example']}`\n\n"

    message += "👉 *Use* `/new <level>` *to start a game with a specific difficulty.* (e.g., `/new hard`)"

    await update.message.reply_text(message, parse_mode='Markdown')

async def clone_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['awaiting_token'] = True
    await update.message.reply_text(
        "👥 **Clone Bot System** 🤖\n"
        "-------------------------------------\n"
        "Create your own Word Challenge game bot in seconds!\n\n"
        "💡 **How to clone:**\n"
        "1️⃣ Go to @BotFather and send `/newbot`.\n"
        "2️⃣ Choose a Name and a Username for your bot.\n"
        "3️⃣ Copy the **HTTP API Token** (e.g., `12345678:ABCDefGh...`).\n"
        "4️⃣ **Send the token directly in the chat below!**\n\n"
        "⚠️ *Note: Only 1 cloned bot is allowed per Telegram user.*",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]]),
        parse_mode='Markdown'
    )

async def manage_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not mongo_manager:
        await update.message.reply_text("❌ Database error.")
        return
    clone = mongo_manager.get_clone_by_owner(user_id)
    if not clone:
        await update.message.reply_text(
            "❌ **You do not have any active clone!**\n\n"
            "Use `/clone` to create one.",
            parse_mode='Markdown'
        )
        return

    # Get custom links
    channel = clone.get("custom_channel") or "None (Default)"
    support = clone.get("custom_group") or "None (Default)"

    await update.message.reply_text(
        f"⚙️ **Manage Your Clone Bot**\n"
        f"-------------------------------------\n"
        f"🤖 **Bot Username:** @{clone['bot_username']}\n"
        f"🟢 **Status:** Running\n"
        f"📢 **Channel Link:** {channel}\n"
        f"💬 **Support Link:** {support}\n\n"
        f"Choose an action below to manage your bot:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Broadcast Message", callback_data="manage_broadcast")],
            [InlineKeyboardButton("🔗 Edit Channel Link", callback_data="manage_edit_channel"),
             InlineKeyboardButton("💬 Edit Support Link", callback_data="manage_edit_support")],
            [InlineKeyboardButton("🛑 Stop & Delete Clone", callback_data="manage_delete_clone")],
            [InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]
        ]),
        parse_mode='Markdown'
    )


# --- Admin Panel and Broadcast Commands (Admin only) ---

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Main owner admin panel."""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    if not mongo_manager:
        await update.message.reply_text("❌ Database not connected.")
        return

    clones = mongo_manager.get_all_clones()
    active_clones = list(clone_manager.clones.values()) if clone_manager else []

    # Check args
    if context.args:
        subcmd = context.args[0].lower()
        if subcmd == "list":
            msg = "📋 **List of Cloned Bots:**\n"
            for c in clones:
                msg += f"- @{c['bot_username']} (Owner ID: `{c['user_id']}`)\n"
            await update.message.reply_text(msg, parse_mode='Markdown')
            return
        elif subcmd == "stop":
            if len(context.args) < 2:
                await update.message.reply_text("Usage: `/admin stop <bot_username>`", parse_mode='Markdown')
                return
            target = context.args[1].lower().replace("@", "")
            clone = mongo_manager.get_clone_by_username(target)
            if clone:
                await clone_manager.stop_clone(clone['token'])
                mongo_manager.delete_clone(clone['user_id'])
                await update.message.reply_text(f"✅ Bot @{target} has been stopped and deleted.")
            else:
                await update.message.reply_text(f"❌ Bot @{target} not found in database.")
            return

    msg = (
        "👑 **Word Challenge Admin Control Panel** 👑\n"
        "-------------------------------------\n"
        f"👥 Total Clones in DB: **{len(clones)}**\n"
        f"🟢 Active Clones Running: **{len(active_clones)}**\n\n"
        "Commands:\n"
        "👉 `/admin list` - List all cloned bots\n"
        "👉 `/admin stop <username>` - Stop and delete a cloned bot\n"
        "👉 `/broadcast <message>` - Broadcast to ALL users on ALL cloned bots"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a message to all known chats across all cloned bots and main bot (Admin only)."""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    if not context.args:
        await update.message.reply_text("Usage: `/broadcast <your message here>`", parse_mode='Markdown')
        return
    
    if not mongo_manager:
        await update.message.reply_text("❌ Database error. Cannot retrieve chat list.")
        return

    message_to_send = " ".join(context.args)
    chats = mongo_manager.get_all_chats()
    
    success_count = 0
    fail_count = 0
    
    await update.message.reply_text(f"📢 *Attempting global cross-bot broadcast to* **{len(chats)}** *chats...*")

    main_bot_username = context.bot.username.lower()

    for chat in chats:
        chat_id = chat['chat_id']
        target_bot_username = chat.get('bot_username', main_bot_username).lower()

        # Determine which bot instance to use
        bot_instance = None
        if target_bot_username == main_bot_username:
            bot_instance = context.bot
        else:
            # Look up in clone manager
            clone_app = clone_manager.clones_by_username.get(target_bot_username) if clone_manager else None
            if clone_app:
                bot_instance = clone_app.bot

        if not bot_instance:
            logger.warning(f"No running bot instance found for broadcast to {chat_id} on @{target_bot_username}")
            fail_count += 1
            continue

        try:
            await bot_instance.send_message(chat_id=chat_id, text=message_to_send, parse_mode='Markdown')
            success_count += 1
        except error.Forbidden:
            logger.warning(f"Failed to send broadcast to chat {chat_id} via @{target_bot_username}: Bot blocked.")
            fail_count += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to chat {chat_id} via @{target_bot_username}: {e}")
            fail_count += 1
            
    await update.message.reply_text(
        f"✅ **Global Broadcast Complete**\n"
        f"Successful: **{success_count}**\n"
        f"Failed: **{fail_count}**",
        parse_mode='Markdown'
    )

# --- Callback Handler (Unchanged) ---
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer() 
    chat_id = query.message.chat_id
    bot_username = context.bot.username.lower()
    
    if query.data == "back_to_start":
        channel_url, group_url = await get_bot_keyboard_links(bot_username)
        await query.edit_message_text(
            f"👋 *Hello! I'm* **@{context.bot.username}** 🤖\n"
            f"-------------------------------------\n"
            f"The **Ultimate Word Challenge** on Telegram!\n\n"
            f"📜 **Goal:** *Guess the secret word using hints (🟩/🟨/🟥).*\n"
            f"🏆 **Compete:** *Win to earn points and climb the Global Leaderboard!* 🌐\n\n"
            f"👉 Tap **/new** or the button below to start your challenge!\n"
            f"-------------------------------------",
            reply_markup=get_start_keyboard(context.bot.username, channel_url, group_url),
            parse_mode='Markdown'
        )
    
    elif query.data == "show_help_menu":
        await query.edit_message_text(
            "📖 **Word Challenge Help Center**\n"
            "-------------------------------------\n"
            "*Choose a topic below to get assistance.*\n"
            "*For any issue, please ask in the Support group!*",
            reply_markup=get_help_menu_keyboard(),
            parse_mode='Markdown'
        )

    elif query.data == "show_how_to_play":
        commands_list = (
            "🤔 **How to Play Word Challenge** ❓\n"
            "-------------------------------------\n"
            "1. **The Word:** *Guess a secret word*, length depends on difficulty (4, 5, or 8 letters).\n\n"
            "2. **The Hints (`Boxes - Word`):**\n"
            "   • 🟢 *Green* = Correct letter, **Right Place**.\n"
            "   • 🟡 *Yellow* = Correct letter, **Wrong Place**.\n"
            "   • 🔴 *Red* = Letter **Not in the Word**.\n\n"
            "3. **The Game:** You have *30 guesses*. The person who wins with the fewest guesses gets the most points! 🥇"
        )
        await query.edit_message_text(commands_list, reply_markup=get_help_menu_keyboard(), parse_mode='Markdown')

    elif query.data == "show_commands":
        commands_list = (
            "📘 **Word Challenge Commands List**\n"
            "-------------------------------------\n"
            "• **/new** [difficulty] → *Start a game*.\n"
            "• **/status** → *Show current game status and history*.\n"
            "• **/leaderboard** [period] → *Show global/daily/weekly/monthly rankings*.\n"
            "• **/end** → *End current game* (Admin Only / DM).\n"
            "• **/difficulty** → *Show difficulty settings* (Admin Only / DM).\n"
            "• **/clone** → *Clone this bot to your own bot*.\n"
            "• **/manage** → *Manage your cloned bot settings*.\n"
        )
        await query.edit_message_text(commands_list, reply_markup=get_help_menu_keyboard(), parse_mode='Markdown')
        
    elif query.data == "show_leaderboard_menu":
        await query.edit_message_text(
            "🏆 **Leaderboard Selection**\n"
            "-------------------------------------\n"
            "*Select the ranking period you wish to view.*",
            reply_markup=get_leaderboard_menu_keyboard(),
            parse_mode='Markdown'
        )
        
    elif query.data.startswith("show_leaderboard_"):
        period = query.data.split('_')[-1]
        await display_leaderboard(update, context, period)

    elif query.data == "new_game_menu":
        await query.edit_message_text(
            "🎯 **Select Your Challenge Level:**\n"
            "*Choose the word length and point value.*",
            reply_markup=get_new_game_keyboard(),
            parse_mode='Markdown'
        )
    
    elif query.data.startswith("start_"):
        difficulty = query.data.split('_')[1]
        
        if mongo_manager and mongo_manager.get_game_state(chat_id, bot_username):
            await query.edit_message_text("⏳ *A game is already active*. Use **/end** to stop it first.")
            return

        success, message = await start_new_game_logic(chat_id, difficulty, bot_username)
        if success:
            await query.edit_message_text(message, parse_mode='Markdown')
        else:
            await query.edit_message_text(f"❌ *Game start failed*: {message}", parse_mode='Markdown')

    elif query.data == "clone_menu":
        context.user_data['awaiting_token'] = True
        await query.edit_message_text(
            "👥 **Clone Bot System** 🤖\n"
            "-------------------------------------\n"
            "Create your own Word Challenge game bot in seconds!\n\n"
            "💡 **How to clone:**\n"
            "1️⃣ Go to @BotFather and send `/newbot`.\n"
            "2️⃣ Choose a Name and a Username for your bot.\n"
            "3️⃣ Copy the **HTTP API Token** (e.g., `12345678:ABCDefGh...`).\n"
            "4️⃣ **Send the token directly in the chat below!**\n\n"
            "⚠️ *Note: Only 1 cloned bot is allowed per Telegram user.*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]]),
            parse_mode='Markdown'
        )

    elif query.data == "manage_menu":
        if not mongo_manager:
            await query.answer("❌ Database error.")
            return
        clone = mongo_manager.get_clone_by_owner(query.from_user.id)
        if not clone:
            await query.edit_message_text(
                "❌ **You do not have any active clone!**\n\n"
                "Tap '👥 Clone Bot' in the main menu or send `/clone` to create one.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("👥 Clone Bot", callback_data="clone_menu")],
                    [InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]
                ]),
                parse_mode='Markdown'
            )
            return

        channel = clone.get("custom_channel") or "None (Default)"
        support = clone.get("custom_group") or "None (Default)"

        await query.edit_message_text(
            f"⚙️ **Manage Your Clone Bot**\n"
            f"-------------------------------------\n"
            f"🤖 **Bot Username:** @{clone['bot_username']}\n"
            f"🟢 **Status:** Running\n"
            f"📢 **Channel Link:** {channel}\n"
            f"💬 **Support Link:** {support}\n\n"
            f"Choose an action below to manage your bot:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Broadcast Message", callback_data="manage_broadcast")],
                [InlineKeyboardButton("🔗 Edit Channel Link", callback_data="manage_edit_channel"),
                 InlineKeyboardButton("💬 Edit Support Link", callback_data="manage_edit_support")],
                [InlineKeyboardButton("🛑 Stop & Delete Clone", callback_data="manage_delete_clone")],
                [InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]
            ]),
            parse_mode='Markdown'
        )

    elif query.data == "manage_broadcast":
        context.user_data['awaiting_broadcast'] = True
        await query.edit_message_text(
            "📢 **Clone Broadcast**\n"
            "-------------------------------------\n"
            "Please send the message you want to broadcast to all users of your cloned bot.\n\n"
            "👉 *Send the message text directly in this chat!*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manage", callback_data="manage_menu")]]),
            parse_mode='Markdown'
        )

    elif query.data == "manage_edit_channel":
        context.user_data['awaiting_channel'] = True
        await query.edit_message_text(
            "🔗 **Set Custom Updates Channel**\n"
            "-------------------------------------\n"
            "Please send your custom Telegram channel link (e.g., `https://t.me/mychannel`).\n\n"
            "👉 *Send the link directly in this chat!*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manage", callback_data="manage_menu")]]),
            parse_mode='Markdown'
        )

    elif query.data == "manage_edit_support":
        context.user_data['awaiting_support'] = True
        await query.edit_message_text(
            "💬 **Set Custom Support Group**\n"
            "-------------------------------------\n"
            "Please send your custom Telegram support/chat group link (e.g., `https://t.me/mygroup`).\n\n"
            "👉 *Send the link directly in this chat!*",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Manage", callback_data="manage_menu")]]),
            parse_mode='Markdown'
        )

    elif query.data == "manage_delete_clone":
        if not mongo_manager: return
        clone = mongo_manager.get_clone_by_owner(query.from_user.id)
        if clone:
            await clone_manager.stop_clone(clone['token'])
            mongo_manager.delete_clone(query.from_user.id)
            await query.edit_message_text(
                "🛑 **Clone Bot Stopped & Deleted**\n"
                "-------------------------------------\n"
                "Your clone bot has been successfully stopped and deleted from our servers.\n\n"
                "You can create a new clone anytime!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Start", callback_data="back_to_start")]]),
                parse_mode='Markdown'
            )
        else:
            await query.answer("❌ No clone bot found.")

# --- Updated Guess Handler (Handles the new error message) ---

async def handle_guess(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = update.effective_user
    guess = update.message.text.strip()
    bot_username = context.bot.username.lower()
    
    username = user.username or user.first_name
    logger.info(f"Guess received in chat {chat_id} from {username}: {guess}")

    if not mongo_manager:
        return

    game_state = mongo_manager.get_game_state(chat_id, bot_username)
    if not game_state:
        return 

    # Process guess
    feedback, is_win, status_message, points, guess_history = await process_guess_logic(chat_id, guess, bot_username)
    
    # 1. Handle validation errors (Incorrect length OR Duplicate guess)
    if status_message.startswith("❌"):
        await update.message.reply_text(status_message, parse_mode='Markdown')
        return

    reply_markup = None
    
    # 2. Construct the full history display
    game_history_display = "\n".join(guess_history)
    
    # 3. Handle Win/Loss/Ongoing
    
    if is_win:
        word_was = guess_history[-1].split(' - ')[-1].replace('**', '').strip() 
        
        # This function now updates global, daily, weekly, and monthly scores
        mongo_manager.update_leaderboard(user.id, username, points) 
        
        reply_text = (
            f"**🏆 GAME WON! 🥳**\n"
            f"-------------------------------------\n"
            f"*Congratulations* **{username}**!\n"
            f"You cracked the code in **{len(guess_history)}** attempts!\n"
            f"✨ Points earned: **`{points}`**\n\n"
            f"📜 **Final Board:**\n"
            f"{game_history_display}\n\n" 
            f"✅ *The secret word was:* **`{word_was}`**"
        )
        reply_markup = get_play_again_keyboard()

    elif status_message.startswith("LOSS_WORD:"):
        word_was = status_message.split(":")[1]
        
        reply_text = (
            f"💔 **GAME OVER! 😭**\n"
            f"-------------------------------------\n"
            f"*Maximum guesses reached* (**{game_state['max_guesses']}**).\n\n"
            f"📜 **Final Board:**\n"
            f"{game_history_display}\n\n" 
            f"❌ *The secret word was:* **`{word_was}`**"
        )
        reply_markup = get_play_again_keyboard()

    else:
        # Ongoing game message (Show full history + status)
        
        reply_text = (
            f"**Word Challenge** 🎯\n"
            f"-------------------------------------\n"
            f"Attempts: **`{len(guess_history)}`** / **`{game_state['max_guesses']}`**\n\n"
            f"📜 **Guess History:**\n"
            f"{game_history_display}\n\n" 
            f"👉 {status_message}" # Displays: Guesses left: **27**
        )
    
    await update.message.reply_text(
        reply_text, 
        reply_markup=reply_markup, 
        parse_mode='Markdown'
    )

# --- Integrated Message Router ---

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bot_username = context.bot.username.lower()
    chat_id = update.effective_chat.id
    user = update.effective_user
    text = update.message.text.strip() if update.message and update.message.text else ""
    
    if not text:
        return

    if mongo_manager:
        mongo_manager.add_chat(chat_id, update.effective_chat.type.name, update.message.date.timestamp(), bot_username)

    # 1. State: Awaiting Bot Token for Clone
    if context.user_data.get('awaiting_token'):
        context.user_data['awaiting_token'] = False
        await update.message.reply_text("⏳ *Validating and starting your clone bot... please wait*", parse_mode='Markdown')
        success, res = await clone_manager.start_clone(text, user.id)
        if success:
            mongo_manager.save_clone(user.id, user.username or user.first_name, text, res)
            await update.message.reply_text(
                f"🎉 **Bot Cloned Successfully!**\n\n"
                f"🔗 Your bot is now active at: @{res}\n"
                f"👉 Use `/manage` in the main bot to manage your clone and run broadcasts!",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                f"❌ **Failed to Clone Bot**\n\n"
                f"Error: `{res}`\n\n"
                f"Please ensure you copied the exact HTTP API Token from @BotFather and try again.",
                parse_mode='Markdown'
            )
        return

    # 2. State: Awaiting Clone Broadcast message
    if context.user_data.get('awaiting_broadcast'):
        context.user_data['awaiting_broadcast'] = False
        await update.message.reply_text("📢 *Sending broadcast to all your clone's users...*", parse_mode='Markdown')

        clone = mongo_manager.get_clone_by_owner(user.id)
        if not clone:
            await update.message.reply_text("❌ Clone bot not found.")
            return

        clone_bot_username = clone['bot_username']
        chats = mongo_manager.get_all_chats(clone_bot_username)

        # Get the clone bot application
        clone_app = clone_manager.clones_by_username.get(clone_bot_username)
        if not clone_app:
            await update.message.reply_text("❌ Clone bot application is not currently running. Start it first.")
            return

        success_count = 0
        fail_count = 0

        for chat in chats:
            try:
                await clone_app.bot.send_message(chat_id=chat['chat_id'], text=text, parse_mode='Markdown')
                success_count += 1
            except Exception:
                fail_count += 1

        await update.message.reply_text(
            f"✅ **Broadcast Complete**\n"
            f"Successful: **{success_count}**\n"
            f"Failed: **{fail_count}**",
            parse_mode='Markdown'
        )
        return

    # 3. State: Awaiting Custom Channel Link
    if context.user_data.get('awaiting_channel'):
        context.user_data['awaiting_channel'] = False
        if not (text.startswith("https://t.me/") or text.startswith("http://t.me/")):
            await update.message.reply_text("❌ *Invalid URL*. Must start with `https://t.me/`", parse_mode='Markdown')
            return
        mongo_manager.update_clone_links(user.id, custom_channel=text)
        await update.message.reply_text(f"✅ **Custom channel link updated to:** {text}", parse_mode='Markdown')
        return

    # 4. State: Awaiting Custom Support Link
    if context.user_data.get('awaiting_support'):
        context.user_data['awaiting_support'] = False
        if not (text.startswith("https://t.me/") or text.startswith("http://t.me/")):
            await update.message.reply_text("❌ *Invalid URL*. Must start with `https://t.me/`", parse_mode='Markdown')
            return
        mongo_manager.update_clone_links(user.id, custom_group=text)
        await update.message.reply_text(f"✅ **Custom support link updated to:** {text}", parse_mode='Markdown')
        return

    # 5. Check if guess (1-8 alphabetic characters) and there's an active game
    if re.match(r'^[a-zA-Z]{1,8}$', text):
        if mongo_manager and mongo_manager.get_game_state(chat_id, bot_username):
            await handle_guess(update, context)
            return

    # If in private chat, show friendly command help
    if update.effective_chat.type == ChatType.PRIVATE:
        await update.message.reply_text("❓ *Command not recognized*. Use `/start` to see the main menu.", parse_mode='Markdown')

# --- Unified Handler Registration & Lifecycle Hooks ---

async def track_all_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Saves any interacting user's ID into the database for the current chat."""
    if not mongo_manager:
        return

    bot_username = context.bot.username.lower()

    # 1. Check if it's a chat_member update
    if update.chat_member:
        chat = update.chat_member.chat
        user = update.chat_member.new_chat_member.user
        if chat and user and chat.type in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
            mongo_manager.save_chat_member(chat.id, user.id, bot_username)
            from_user = update.chat_member.from_user
            if from_user:
                mongo_manager.save_chat_member(chat.id, from_user.id, bot_username)

    # 2. Check standard effective_chat and effective_user
    chat = update.effective_chat
    user = update.effective_user
    if chat and user:
        if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
            mongo_manager.save_chat_member(chat.id, user.id, bot_username)

async def ban_single_user(bot, chat_id, user_id):
    try:
        await bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        return True
    except error.RetryAfter as e:
        logger.warning(f"Rate limited for user {user_id}: retry after {e.retry_after}")
        await asyncio.sleep(e.retry_after)
        try:
            await bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            return True
        except Exception:
            return False
    except Exception as e:
        logger.debug(f"Failed to ban user {user_id}: {e}")
        return False

async def unban_single_user(bot, chat_id, user_id):
    try:
        await bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
        return True
    except error.RetryAfter as e:
        logger.warning(f"Rate limited for user {user_id}: retry after {e.retry_after}")
        await asyncio.sleep(e.retry_after)
        try:
            await bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            return True
        except Exception:
            return False
    except Exception as e:
        logger.debug(f"Failed to unban user {user_id}: {e}")
        return False

async def banall_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not mongo_manager:
        return

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    bot_username = context.bot.username.lower()

    # Auth check
    is_authorized = False
    clone = mongo_manager.get_clone_by_username(bot_username)
    if clone:
        if user_id == clone.get("user_id"):
            is_authorized = True
    else:
        if user_id == ADMIN_USER_ID:
            is_authorized = True

    if not is_authorized:
        return

    if update.effective_chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
        await update.message.reply_text("🚨 This command can only be used in groups, supergroups, or channels.")
        return

    # Fetch administrators
    admin_ids = set()
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        for admin in admins:
            admin_ids.add(admin.user.id)
    except Exception as e:
        logger.warning(f"Could not retrieve chat administrators: {e}")

    # Fetch all tracked members
    tracked_members = mongo_manager.get_chat_members(chat_id, bot_username)
    bot_id = context.bot.id
    targets = [uid for uid in tracked_members if uid not in admin_ids and uid != user_id and uid != bot_id]

    if not targets:
        await update.message.reply_text("⚠️ No tracked members found to ban.")
        return

    status_msg = await update.message.reply_text(f"⚡ Starting speed ban of {len(targets)} members...")

    # Run concurrently
    tasks = [ban_single_user(context.bot, chat_id, uid) for uid in targets]
    results = await asyncio.gather(*tasks)

    success_count = sum(1 for r in results if r)
    fail_count = len(targets) - success_count

    await status_msg.edit_text(
        f"✅ **Speed Ban Completed!**\n"
        f"-------------------------------------\n"
        f"👥 Total processed: **{len(targets)}**\n"
        f"🟢 Successfully banned: **{success_count}**\n"
        f"🔴 Failed / already banned: **{fail_count}**",
        parse_mode='Markdown'
    )

async def unbanall_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not mongo_manager:
        return

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    bot_username = context.bot.username.lower()

    # Auth check
    is_authorized = False
    clone = mongo_manager.get_clone_by_username(bot_username)
    if clone:
        if user_id == clone.get("user_id"):
            is_authorized = True
    else:
        if user_id == ADMIN_USER_ID:
            is_authorized = True

    if not is_authorized:
        return

    if update.effective_chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
        await update.message.reply_text("🚨 This command can only be used in groups, supergroups, or channels.")
        return

    # Fetch administrators
    admin_ids = set()
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        for admin in admins:
            admin_ids.add(admin.user.id)
    except Exception as e:
        logger.warning(f"Could not retrieve chat administrators: {e}")

    # Fetch all tracked members
    tracked_members = mongo_manager.get_chat_members(chat_id, bot_username)
    bot_id = context.bot.id
    targets = [uid for uid in tracked_members if uid not in admin_ids and uid != user_id and uid != bot_id]

    if not targets:
        await update.message.reply_text("⚠️ No tracked members found to unban.")
        return

    status_msg = await update.message.reply_text(f"⚡ Starting speed unban of {len(targets)} members...")

    # Run concurrently
    tasks = [unban_single_user(context.bot, chat_id, uid) for uid in targets]
    results = await asyncio.gather(*tasks)

    success_count = sum(1 for r in results if r)
    fail_count = len(targets) - success_count

    await status_msg.edit_text(
        f"✅ **Speed Unban Completed!**\n"
        f"-------------------------------------\n"
        f"👥 Total processed: **{len(targets)}**\n"
        f"🟢 Successfully unbanned: **{success_count}**\n"
        f"🔴 Failed / already unbanned: **{fail_count}**",
        parse_mode='Markdown'
    )

def register_all_handlers(application: Application):
    # Track all updates in group=-1 to avoid interfering with command/message handlers
    application.add_handler(TypeHandler(Update, track_all_updates), group=-1)

    application.add_handler(CommandHandler("banall", banall_command))
    application.add_handler(CommandHandler("unbanall", unbanall_command))
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("new", new_game_command))
    application.add_handler(CommandHandler("end", end_game_command))
    application.add_handler(CommandHandler("leaderboard", leaderboard_command))
    application.add_handler(CommandHandler("difficulty", difficulty_command)) 
    application.add_handler(CommandHandler("status", status_command)) 
    application.add_handler(CommandHandler("clone", clone_command))
    application.add_handler(CommandHandler("manage", manage_command))
    application.add_handler(CommandHandler("admin", admin_command))
    
    if ADMIN_USER_ID != 0 and mongo_manager is not None:
        application.add_handler(CommandHandler("broadcast", broadcast_command))
    
    application.add_handler(CommandHandler("help", start_command)) 

    # Callback query handler for inline buttons
    application.add_handler(CallbackQueryHandler(callback_handler))
    
    # Message handler for guesses and multi-step state text inputs:
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_message
        )
    )

async def post_init(application: Application) -> None:
    if clone_manager:
        asyncio.create_task(clone_manager.start_all())

async def post_shutdown(application: Application) -> None:
    if clone_manager:
        await clone_manager.stop_all()

# --- Main Bot Runner ---

def main():
    """Start the bot."""
    if not BOT_TOKEN:
        logger.error("FATAL ERROR: BOT_TOKEN not found. Please set it in the .env file.")
        return

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    register_all_handlers(application)

    logger.info("🚀 Word Challenge Bot is running (Clone Engine & Leaderboards Ready)...")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
