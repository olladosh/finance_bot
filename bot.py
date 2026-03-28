import asyncio
import logging
import sqlite3
import random
import re
import time
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ========== ТОКЕН БОТА (ВСТАВЛЕН ПРЯМО ЗДЕСЬ) ==========
BOT_TOKEN = "8623084217:AAFq-LwPvNcsm0hVZ4KHwSA7dFJI8lVqo4A"

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== БАЗА ДАННЫХ ==========

def execute_with_retry(func, *args, max_retries=10, **kwargs):
    for attempt in range(max_retries):
        conn = None
        try:
            conn = sqlite3.connect("/app/data/finance.db", timeout=120)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=120000")
            conn.execute("PRAGMA synchronous=OFF")
            cursor = conn.cursor()
            result = func(cursor, *args, **kwargs)
            conn.commit()
            return result
        except sqlite3.OperationalError as e:
            if conn:
                conn.close()
            if "database is locked" in str(e) and attempt < max_retries - 1:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise
        finally:
            if conn:
                conn.close()

def init_db():
    def _init(cursor):
        cursor.execute('''CREATE TABLE IF NOT EXISTS transactions 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  user_id INTEGER, 
                  type TEXT, 
                  amount REAL, 
                  category TEXT, 
                  date TEXT)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS budgets 
                 (user_id INTEGER PRIMARY KEY, 
                  monthly_budget REAL DEFAULT 0)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS achievements 
                 (user_id INTEGER, 
                  achievement TEXT, 
                  earned_date TEXT, 
                  UNIQUE(user_id, achievement))''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS tracking_stats 
                 (user_id INTEGER PRIMARY KEY, 
                  last_activity_date TEXT, 
                  streak_days INTEGER DEFAULT 0)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS subscribers 
                 (user_id INTEGER PRIMARY KEY, 
                  username TEXT, 
                  first_name TEXT, 
                  subscribed_at TEXT, 
                  last_sent_date TEXT)''')
    execute_with_retry(_init)

init_db()

# ========== ПАРСИНГ СУММЫ ==========

def parse_amount(text: str) -> float:
    text = text.replace(" ", "").replace(",", "")
    numbers = re.findall(r"[\d\.]+", text)
    if not numbers:
        return None
    try:
        return float(numbers[0])
    except:
        return None

# ========== ВОПРОСЫ ВИКТОРИНЫ (15 ШТУК) ==========

QUIZ_QUESTIONS = [
    {"question": "Сколько месяцев расходов должна составлять финансовая подушка безопасности?", "options": ["1 месяц", "3-6 месяцев", "12 месяцев", "Не нужна"], "correct": "3-6 месяцев", "explanation": "Подушка безопасности должна покрывать 3-6 месяцев расходов."},
    {"question": "Сколько можно вернуть налогового вычета при покупке квартиры (максимум на человека)?", "options": ["100 000 ₽", "260 000 ₽", "500 000 ₽", "1 000 000 ₽"], "correct": "260 000 ₽", "explanation": "13% от стоимости квартиры, максимум 260 000 ₽."},
    {"question": "Что такое правило 50/30/20?", "options": ["50% на еду, 30% на одежду, 20% на развлечения", "50% на обязательные траты, 30% на желания, 20% на накопления", "50% на кредиты, 30% на жилье, 20% на сбережения", "50% на инвестиции, 30% на налоги, 20% на благотворительность"], "correct": "50% на обязательные траты, 30% на желания, 20% на накопления", "explanation": "Классическое правило распределения бюджета."},
    {"question": "Что выгоднее: досрочно погасить ипотеку или положить деньги на вклад?", "options": ["Всегда выгоднее гасить ипотеку", "Всегда выгоднее класть на вклад", "Зависит от процентной ставки", "Ни то, ни другое"], "correct": "Зависит от процентной ставки", "explanation": "Если ставка по ипотеке ниже доходности вклада — выгоднее копить."},
    {"question": "Что такое сложный процент?", "options": ["Процент на кредит", "Процент на процент, когда доход реинвестируется", "Высокий банковский процент", "Пени за просрочку"], "correct": "Процент на процент, когда доход реинвестируется", "explanation": "Сложный процент — это когда проценты начисляются на уже накопленные проценты."},
    {"question": "Какой процент от дохода рекомендуется откладывать на накопления?", "options": ["5-10%", "10-20%", "20-30%", "50%"], "correct": "10-20%", "explanation": "Рекомендуется откладывать 10-20% дохода."},
    {"question": "Что такое инфляция?", "options": ["Рост цен на товары и услуги", "Снижение курса рубля", "Увеличение зарплат", "Банковский процент"], "correct": "Рост цен на товары и услуги", "explanation": "Инфляция — это обесценивание денег."},
    {"question": "Что такое кредитная история?", "options": ["История всех ваших покупок", "Информация о ваших кредитах и платежах", "Список ваших счетов в банках", "Данные о ваших доходах"], "correct": "Информация о ваших кредитах и платежах", "explanation": "Кредитная история влияет на одобрение кредитов."},
    {"question": "Что такое диверсификация инвестиций?", "options": ["Вложение всех денег в один актив", "Распределение денег между разными активами", "Покупка только акций", "Инвестирование на короткий срок"], "correct": "Распределение денег между разными активами", "explanation": "Диверсификация помогает снизить риски."},
    {"question": "Что такое пассивный доход?", "options": ["Зарплата на работе", "Доход, который приходит без активных действий", "Подарки от родственников", "Выигрыш в лотерею"], "correct": "Доход, который приходит без активных действий", "explanation": "Примеры: аренда, дивиденды, проценты по вкладам."},
    {"question": "Какой максимальный размер страхового возмещения по вкладам в России?", "options": ["700 000 ₽", "1 000 000 ₽", "1 400 000 ₽", "2 000 000 ₽"], "correct": "1 400 000 ₽", "explanation": "С 2021 года максимальная сумма страховки — 1,4 млн рублей."},
    {"question": "Что такое рефинансирование кредита?", "options": ["Увеличение суммы кредита", "Новый кредит для погашения старого на лучших условиях", "Отказ от выплаты кредита", "Продажа залога"], "correct": "Новый кредит для погашения старого на лучших условиях", "explanation": "Рефинансирование помогает снизить процентную ставку."},
    {"question": "Что такое ИИС (индивидуальный инвестиционный счет)?", "options": ["Счет для покупки недвижимости", "Специальный счет для инвестиций с налоговыми льготами", "Счет для оплаты коммунальных услуг", "Кредитный счет"], "correct": "Специальный счет для инвестиций с налоговыми льготами", "explanation": "ИИС дает право на налоговый вычет до 400 000 ₽ в год."},
    {"question": "Что такое финансовый план?", "options": ["Список желаний", "План достижения финансовых целей", "Бюджет на неделю", "Список кредитов"], "correct": "План достижения финансовых целей", "explanation": "Финансовый план помогает достигать целей."},
    {"question": "Что такое активы и пассивы?", "options": ["Доходы и расходы", "То, что приносит деньги, и то, что забирает", "Кредиты и вклады", "Налоги и сборы"], "correct": "То, что приносит деньги, и то, что забирает", "explanation": "Активы приносят доход, пассивы забирают деньги."}
]

# ========== СОВЕТЫ ДНЯ ==========

DAILY_TIPS = [
    "💡 Финансовая подушка безопасности должна составлять 3-6 месяцев ваших расходов.",
    "💡 Используй налоговый вычет! При покупке квартиры можно вернуть до 650 000 ₽.",
    "💡 Ведите совместный бюджет. Это снижает конфликты.",
    "💡 Кредитная карта — не дополнительные деньги.",
    "💡 Открой накопительный счет. Даже 500 ₽ в месяц создадут подушку.",
    "💡 Правило 50/30/20: 50% на обязательные траты, 30% на желания, 20% на накопления.",
    "💡 Не бери кредит на свадьбу. Лучше отпразднуйте скромнее.",
    "💡 Перед покупкой дорогой вещи подожди 24 часа.",
    "💡 Инвестируй в себя! Образование — лучшая инвестиция.",
    "💡 Сравнивай цены в разных магазинах. Экономия до 30%!",
]

# ========== ДОСТИЖЕНИЯ ==========

ACHIEVEMENTS = {
    "first_income": {"name": "💰 Первый доход", "emoji": "💰", "desc": "Добавил первый доход"},
    "first_expense": {"name": "🛒 Первый расход", "emoji": "🛒", "desc": "Добавил первый расход"},
    "ten_transactions": {"name": "🔥 10 транзакций", "emoji": "🔥", "desc": "Добавил 10 записей"},
    "saver": {"name": "💎 Копильщик", "emoji": "💎", "desc": "Сберег больше 10 000 ₽"}
}

async def check_achievements(user_id, message):
    def _check(cursor):
        cursor.execute("SELECT achievement FROM achievements WHERE user_id=?", (user_id,))
        earned = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM transactions WHERE user_id=? AND type='expense'", (user_id,))
        count, total_expense = cursor.fetchone()
        count = count or 0
        total_expense = total_expense or 0
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE user_id=? AND type='income'", (user_id,))
        income_count = cursor.fetchone()[0] or 0
        new = []
        if income_count >= 1 and "first_income" not in earned:
            cursor.execute("INSERT INTO achievements VALUES (?, ?, ?)", (user_id, "first_income", datetime.now().strftime("%Y-%m-%d")))
            new.append(ACHIEVEMENTS["first_income"])
        if count >= 1 and "first_expense" not in earned:
            cursor.execute("INSERT INTO achievements VALUES (?, ?, ?)", (user_id, "first_expense", datetime.now().strftime("%Y-%m-%d")))
            new.append(ACHIEVEMENTS["first_expense"])
        if (count + income_count) >= 10 and "ten_transactions" not in earned:
            cursor.execute("INSERT INTO achievements VALUES (?, ?, ?)", (user_id, "ten_transactions", datetime.now().strftime("%Y-%m-%d")))
            new.append(ACHIEVEMENTS["ten_transactions"])
        if total_expense > 10000 and "saver" not in earned:
            cursor.execute("INSERT INTO achievements VALUES (?, ?, ?)", (user_id, "saver", datetime.now().strftime("%Y-%m-%d")))
            new.append(ACHIEVEMENTS["saver"])
        return new
    new = execute_with_retry(_check)
    for ach in new:
        await message.answer(f"🏆 НОВОЕ ДОСТИЖЕНИЕ! 🏆\n\n{ach['emoji']} {ach['name']}\n{ach['desc']}", parse_mode=ParseMode.MARKDOWN)

# ========== ПОДПИСКА ==========

def add_subscriber(user_id, username, first_name):
    def _add(cursor):
        cursor.execute("INSERT OR IGNORE INTO subscribers VALUES (?, ?, ?, ?, ?)", 
                      (user_id, username, first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), None))
    execute_with_retry(_add)

def get_all_subscribers():
    def _get(cursor):
        cursor.execute("SELECT user_id FROM subscribers")
        return cursor.fetchall()
    return execute_with_retry(_get)

def update_last_sent(user_id):
    def _update(cursor):
        cursor.execute("UPDATE subscribers SET last_sent_date = ? WHERE user_id = ?", 
                      (datetime.now().strftime("%Y-%m-%d"), user_id))
    execute_with_retry(_update)

def get_date_header():
    now = datetime.now()
    days = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    return now.day, months[now.month-1], now.year, days[now.weekday()], now.strftime("%H:%M")

async def send_daily_tip():
    subs = get_all_subscribers()
    if not subs:
        return
    tip = random.choice(DAILY_TIPS)
    day, month, year, weekday, time = get_date_header()
    for (user_id,) in subs:
        try:
            await bot.send_message(user_id, f"🌅 ДОБРОЕ УТРО! 🌅\n\n📅 {day} {month} {year}, {weekday}\n⏰ {time}\n\n{tip}\n\n👉 Нажми /start", parse_mode=ParseMode.MARKDOWN)
            update_last_sent(user_id)
        except:
            pass

# ========== КЛАВИАТУРА ==========

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="➕ Добавить доход"), KeyboardButton(text="➖ Добавить расход")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📅 Отчет за месяц")],
        [KeyboardButton(text="💰 Бюджет"), KeyboardButton(text="💡 Совет дня")],
        [KeyboardButton(text="🎮 Викторина"), KeyboardButton(text="🔔 Подписка на советы")],
        [KeyboardButton(text="🎯 11 причин"), KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True
)

# ========== 11 ПРИЧИН ==========

REASONS_TEXT = """💡 11 ВЕСКИХ ПРИЧИН ВЕСТИ УЧЕТ ТРАТ И ДОХОДОВ

Знаю-знаю, учет финансов звучит скучновато... 😅

Но это не про ограничения, а про возможности! 🤔

1️⃣ Найти «утечки» средств 🕳️
2️⃣ Перестать удивляться «куда делись деньги?» 🤔
3️⃣ Попрощаться с тревожностью 🧘🏼
4️⃣ Остановить «долговую карусель» 🎠
5️⃣ Осознанно баловать себя 🥳
6️⃣ Сделать мечту осязаемой ✨
7️⃣ Найти "скрытые" деньги 🕵️‍♂️
8️⃣ Видеть свой прогресс 💪🏽
9️⃣ Увидеть реальную стоимость своего часа ⏳
🔟 Объективные решения на основе фактов ✅
1️⃣1️⃣ Навести порядок в "плавающем" доходе 📊

Начните прямо сейчас! Первый шаг к контролю над финансами — самый важный. 😉"""

# ========== ВИКТОРИНА ==========

quiz_state = {}

@dp.message(lambda msg: msg.text == "🎮 Викторина")
async def start_quiz(message: types.Message):
    user_id = message.from_user.id
    quiz_state[user_id] = {"step": 0, "score": 0}
    await ask_question(message, user_id)

async def ask_question(message: types.Message, user_id: int):
    step = quiz_state[user_id]["step"]
    total = len(QUIZ_QUESTIONS)
    if step >= total:
        score = quiz_state[user_id]["score"]
        await message.answer(
            f"🎉 *ВИКТОРИНА ЗАВЕРШЕНА!* 🎉\n\n"
            f"📊 Твой результат: *{score} из {total}*\n\n"
            f"{'🔥 Отлично!' if score >= total-3 else '💪 Неплохо!'}\n\n"
            f"Нажми '🎮 Викторина' чтобы пройти еще раз!",
            parse_mode=ParseMode.MARKDOWN
        )
        del quiz_state[user_id]
        return
    q = QUIZ_QUESTIONS[step]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=opt, callback_data=f"ans_{opt}")] for opt in q["options"]
    ])
    await message.answer(
        f"🎮 *Вопрос {step+1} из {total}* 🎮\n\n{q['question']}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb
    )

@dp.callback_query(lambda c: c.data and c.data.startswith("ans_"))
async def answer_question(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in quiz_state:
        await callback.message.answer("Начни викторину кнопкой '🎮 Викторина'")
        await callback.answer()
        return
    answer = callback.data.replace("ans_", "")
    step = quiz_state[user_id]["step"]
    q = QUIZ_QUESTIONS[step]
    if answer == q["correct"]:
        quiz_state[user_id]["score"] += 1
        text = f"✅ *Правильно!* {q['explanation']}"
    else:
        text = f"❌ *Неправильно!* Правильный ответ: *{q['correct']}*\n\n{q['explanation']}"
    await callback.message.answer(text, parse_mode=ParseMode.MARKDOWN)
    quiz_state[user_id]["step"] += 1
    await ask_question(callback.message, user_id)
    await callback.answer()

# ========== ОСТАЛЬНЫЕ КОМАНДЫ ==========

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(f"💸 Добро пожаловать в финансовый трекер!\n\n👇 Нажми кнопку ниже:", reply_markup=main_kb, parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "💡 Совет дня")
async def tip_cmd(message: types.Message):
    tip = random.choice(DAILY_TIPS)
    day, month, year, weekday, time = get_date_header()
    await message.answer(f"💡 СОВЕТ ДНЯ 💡\n\n📅 {day} {month} {year}, {weekday}\n⏰ {time}\n\n{tip}", parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "🔔 Подписка на советы")
async def sub_cmd(message: types.Message):
    user_id = message.from_user.id
    def _check(cursor):
        cursor.execute("SELECT user_id FROM subscribers WHERE user_id=?", (user_id,))
        return cursor.fetchone()
    exists = execute_with_retry(_check)
    if exists:
        await message.answer("🔔 Вы уже подписаны! Советы приходят каждый день в 9:00", parse_mode=ParseMode.MARKDOWN)
    else:
        add_subscriber(user_id, message.from_user.username or "", message.from_user.first_name or "")
        await message.answer("✅ Вы подписались на ежедневные советы! Каждое утро в 9:00 будет приходить полезный совет.", parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "🎯 11 причин")
async def reasons_cmd(message: types.Message):
    await message.answer(REASONS_TEXT, parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "❓ Помощь")
async def help_cmd(message: types.Message):
    await message.answer("📖 Как пользоваться:\n\n➕ Доход — выбери категорию и введи сумму\n➖ Расход — выбери категорию и введи сумму\n📊 Статистика — общая статистика\n📅 Отчет за месяц — детальный отчет\n💰 Бюджет — установи лимит\n💡 Совет дня — случайный совет\n🎮 Викторина — 15 вопросов\n🔔 Подписка — ежедневные советы\n🎯 11 причин — мотивация\n\n💡 Вводить сумму можно в любом формате:\n50000, 50.000, 50 000, 50000р, 50000 руб, 50000₽", parse_mode=ParseMode.MARKDOWN)

# ========== ДОХОДЫ/РАСХОДЫ ==========

user_state = {}
income_cats = ["💰 Зарплата", "📈 Инвестиции", "🎁 Подарки", "💸 Фриланс", "🏦 Другое"]
expense_cats = ["🍔 Еда", "🏠 Жилье", "🚗 Транспорт", "📱 Связь", "🛍️ Шопинг", "🎬 Развлечения", "💊 Здоровье", "📚 Образование", "🐶 Другое"]

@dp.message(lambda msg: msg.text == "➕ Добавить доход")
async def inc_cmd(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=cat, callback_data=f"inc_{cat}")] for cat in income_cats])
    await message.answer("💰 Выбери категорию дохода:", reply_markup=kb)

@dp.message(lambda msg: msg.text == "➖ Добавить расход")
async def exp_cmd(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=cat, callback_data=f"exp_{cat}")] for cat in expense_cats])
    await message.answer("🛒 Выбери категорию расхода:", reply_markup=kb)

@dp.callback_query(lambda c: c.data.startswith("inc_") or c.data.startswith("exp_"))
async def cat_selected(callback: types.CallbackQuery):
    if callback.data.startswith("inc_"):
        user_state[callback.from_user.id] = {"type": "income", "category": callback.data.replace("inc_", "")}
        await callback.message.answer("💰 Введи сумму дохода (можно с рублями, пробелами, точками):\n\nПримеры: 50000, 50.000, 50 000, 50000р, 50000 руб, 50000₽")
    else:
        user_state[callback.from_user.id] = {"type": "expense", "category": callback.data.replace("exp_", "")}
        await callback.message.answer("🛒 Введи сумму расхода (можно с рублями, пробелами, точками):\n\nПримеры: 50000, 50.000, 50 000, 50000р, 50000 руб, 50000₽")
    await callback.answer()

@dp.message(lambda msg: msg.from_user.id in user_state)
async def amount_cmd(message: types.Message):
    uid = message.from_user.id
    data = user_state[uid]
    amount = parse_amount(message.text)
    if amount is None:
        await message.answer("❌ Не могу распознать сумму. Введи число, например: 50000, 50.000, 50 000, 50000р")
        return
    if amount > 1_000_000_000:
        await message.answer("❌ Сумма слишком большая! Максимум 1 000 000 000 ₽")
        return
    def _save(cursor):
        cursor.execute("INSERT INTO transactions (user_id, type, amount, category, date) VALUES (?, ?, ?, ?, ?)",
                      (uid, data["type"], amount, data["category"], datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        cursor.execute("SELECT user_id FROM subscribers WHERE user_id=?", (uid,))
        if not cursor.fetchone():
            add_subscriber(uid, message.from_user.username or "", message.from_user.first_name or "")
            return True
        return False
    should_notify = execute_with_retry(_save)
    if should_notify:
        await message.answer("🔔 Бонус! Подписал вас на ежедневные советы в 9:00")
    await check_achievements(uid, message)
    await message.answer(f"{'💰' if data['type']=='income' else '🛒'} {data['category']}: {amount:,.0f} ₽ добавлено!", reply_markup=main_kb)
    del user_state[uid]

# ========== СТАТИСТИКА ==========

@dp.message(lambda msg: msg.text == "📊 Статистика")
async def stats_cmd(message: types.Message):
    uid = message.from_user.id
    def _stats(cursor):
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE user_id=? AND type='income'", (uid,))
        inc = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE user_id=? AND type='expense'", (uid,))
        exp = cursor.fetchone()[0] or 0
        cursor.execute("SELECT category, SUM(amount) FROM transactions WHERE user_id=? AND type='expense' GROUP BY category ORDER BY SUM(amount) DESC LIMIT 5", (uid,))
        top = cursor.fetchall()
        cursor.execute("SELECT streak_days FROM tracking_stats WHERE user_id=?", (uid,))
        streak_row = cursor.fetchone()
        streak = streak_row[0] if streak_row else 0
        return inc, exp, top, streak
    inc, exp, top, streak = execute_with_retry(_stats)
    text = f"📊 *ФИНАНСОВАЯ СТАТИСТИКА*\n\n💰 Доходы: {inc:,.0f} ₽\n🛒 Расходы: {exp:,.0f} ₽\n💎 Баланс: {inc-exp:,.0f} ₽\n🔥 Стрик: {streak} дней\n\n"
    if top:
        text += "🔥 *Топ расходов:*\n"
        for cat, amt in top:
            text += f"• {cat}: {amt:,.0f} ₽\n"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

# ========== ОТЧЕТ ЗА МЕСЯЦ ==========

@dp.message(lambda msg: msg.text == "📅 Отчет за месяц")
async def report_cmd(message: types.Message):
    uid = message.from_user.id
    first = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    def _report(cursor):
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE user_id=? AND type='income' AND date >= ?", (uid, first))
        inc = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE user_id=? AND type='expense' AND date >= ?", (uid, first))
        exp = cursor.fetchone()[0] or 0
        cursor.execute("SELECT category, SUM(amount) FROM transactions WHERE user_id=? AND type='expense' AND date >= ? GROUP BY category", (uid, first))
        cats = cursor.fetchall()
        return inc, exp, cats
    inc, exp, cats = execute_with_retry(_report)
    months = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    text = f"📅 *ОТЧЕТ ЗА {months[datetime.now().month-1].upper()}* 📅\n\n💰 Доходы: {inc:,.0f} ₽\n🛒 Расходы: {exp:,.0f} ₽\n💎 Сбережено: {inc-exp:,.0f} ₽\n\n"
    if cats:
        text += "📂 *По категориям:*\n"
        for cat, amt in cats:
            text += f"• {cat}: {amt:,.0f} ₽\n"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

# ========== БЮДЖЕТ ==========

budget_state = {}

@dp.message(lambda msg: msg.text == "💰 Бюджет")
async def budget_cmd(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Установить бюджет", callback_data="set_b")],
        [InlineKeyboardButton(text="📊 Проверить бюджет", callback_data="check_b")],
        [InlineKeyboardButton(text="🏆 Мои достижения", callback_data="my_ach")]
    ])
    await message.answer("💰 *Управление бюджетом*", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

@dp.callback_query(lambda c: c.data == "set_b")
async def set_b(callback: types.CallbackQuery):
    budget_state[callback.from_user.id] = True
    await callback.message.answer("💰 Введи месячный бюджет (в рублях):\n\nПримеры: 50000, 50.000, 50 000")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "check_b")
async def check_b(callback: types.CallbackQuery):
    uid = callback.from_user.id
    def _check(cursor):
        cursor.execute("SELECT monthly_budget FROM budgets WHERE user_id=?", (uid,))
        b_row = cursor.fetchone()
        b = b_row[0] if b_row else 0
        first = datetime.now().replace(day=1).strftime("%Y-%m-%d")
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE user_id=? AND type='expense' AND date >= ?", (uid, first))
        spent = cursor.fetchone()[0] or 0
        return b, spent
    b, spent = execute_with_retry(_check)
    if b > 0:
        rem = b - spent
        await callback.message.answer(
            f"💰 *Бюджет на месяц:* {b:,.0f} ₽\n"
            f"🛒 *Потрачено:* {spent:,.0f} ₽\n"
            f"💚 *Осталось:* {rem:,.0f} ₽\n\n"
            f"{'✅ Вы в рамках бюджета!' if rem >= 0 else '⚠️ Вы превысили бюджет!'}",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await callback.message.answer("💰 Бюджет не установлен. Нажми 'Установить бюджет'", parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "my_ach")
async def my_ach(callback: types.CallbackQuery):
    uid = callback.from_user.id
    def _get(cursor):
        cursor.execute("SELECT achievement FROM achievements WHERE user_id=?", (uid,))
        return {row[0] for row in cursor.fetchall()}
    earned = execute_with_retry(_get)
    text = "🏆 *МОИ ДОСТИЖЕНИЯ* 🏆\n\n"
    for key, ach in ACHIEVEMENTS.items():
        text += f"{'✅' if key in earned else '⬜'} {ach['emoji']} {ach['name']} — {ach['desc']}\n"
    await callback.message.answer(text, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.message(lambda msg: msg.from_user.id in budget_state)
async def budget_amount(message: types.Message):
    amount = parse_amount(message.text)
    if amount is None:
        await message.answer("❌ Не могу распознать сумму. Введи число, например: 50000, 50.000, 50 000")
        return
    uid = message.from_user.id
    def _save(cursor):
        cursor.execute("INSERT OR REPLACE INTO budgets VALUES (?, ?)", (uid, amount))
    execute_with_retry(_save)
    await message.answer(f"💰 Бюджет установлен: {amount:,.0f} ₽/мес", parse_mode=ParseMode.MARKDOWN)
    del budget_state[uid]

# ========== ЗАПУСК ==========

async def main():
    print("✨ Финансовый трекер запущен!")
    print("✅ Включен WAL-режим и автоматические повторы при блокировках")
    print("✅ 100+ человек могут работать одновременно!")
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_daily_tip, CronTrigger(hour=9, minute=0), id="daily")
    scheduler.start()
    print("⏰ Планировщик запущен, рассылка в 9:00")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())