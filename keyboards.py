from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def generate_options_keyboard(options, question_index, correct_index):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for i, option in enumerate(options):
        is_correct = 1 if i == correct_index else 0
        
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=option,
                callback_data=f"answer_{question_index}_{i}_{is_correct}"
            )
        ])
    
    return keyboard

def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Начать игру"), KeyboardButton(text="Статистика"), KeyboardButton(text="Таблица лидеров")]
        ],
        resize_keyboard=True
    )
    return keyboard