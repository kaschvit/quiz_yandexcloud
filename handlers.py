import logging
from aiogram import types, F, Router
from aiogram.filters.command import Command
from database import *
from keyboards import generate_options_keyboard, get_main_keyboard

logger = logging.getLogger(__name__)

router = Router()

user_quiz_data = {}  
user_scores = {}

async def load_quiz_data_for_user(user_id):
    quiz_data = await get_quiz_data()
    
    if not quiz_data:
        return None
    
    user_quiz_data[user_id] = quiz_data
    return quiz_data

async def get_question(message, user_id):
    current_question_index = await get_quiz_index(user_id)
    
    quiz_data = user_quiz_data.get(user_id)
    
    if not quiz_data:
        await message.answer("Вопросы квиза не загружены")
        return
    
    if current_question_index >= len(quiz_data):
        await message.answer("Квиз завершен!")
        return
    
    question_data = quiz_data[current_question_index]
    
    if not question_data:
        await message.answer("Ошибка: вопрос не найден")
        return
    
    if not question_data.get('question') or not question_data.get('options'):
        await message.answer("Ошибка: некорректные данные вопроса")
        return
    
    correct_index = question_data.get('correct_option', 0)
    opts = question_data.get('options', [])
    
    if not opts:
        await message.answer("Ошибка: варианты ответов не найдены")
        return
        
    kb = generate_options_keyboard(opts, current_question_index, correct_index)
    question_text = f"Вопрос {current_question_index + 1}/{len(quiz_data)}:\n\n{question_data.get('question', '')}"
    
    await message.answer(question_text, reply_markup=kb)

async def new_quiz(message):
    user_id = message.from_user.id
    
    quiz_data = await load_quiz_data_for_user(user_id)
    if not quiz_data:
        await message.answer("Не удалось загрузить вопросы квиза. Попробуйте позже.")
        return
    
    user_scores[user_id] = 0
    await update_quiz_index(user_id, 0)
    
    await get_question(message, user_id)

@router.message(Command("quiz"))
@router.message(F.text == "Начать игру")
async def cmd_quiz(message: types.Message):
    user_id = message.from_user.id
    
    if user_id in user_quiz_data:
        del user_quiz_data[user_id]
    if user_id in user_scores:
        del user_scores[user_id]
    
    await message.answer_photo("https://storage.yandexcloud.net/picturequizbot/images.jpg")
    await message.answer("Начинаем квиз! Загружаем новые вопросы.")
    await new_quiz(message)

@router.callback_query(F.data.startswith("answer_"))
async def process_answer(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    current_question_index = await get_quiz_index(user_id)
    
    quiz_data = user_quiz_data.get(user_id)
    
    if not quiz_data:
        await callback.message.answer("Данные квиза не найдены. Начните заново.")
        return
    
    parts = callback.data.split("_")
    
    if len(parts) != 4:
        await callback.message.answer("Ошибка в данных ответа")
        return
        
    q_index = int(parts[1])
    answer_index = int(parts[2])
    is_correct = bool(int(parts[3]))
    
    if q_index >= len(quiz_data) or q_index < 0:
        await callback.message.answer("Ошибка: данные вопроса не найдены")
        return
        
    question_data = quiz_data[q_index]
    if not question_data:
        await callback.message.answer("Ошибка: данные вопроса не найдены")
        return
    
    await callback.bot.edit_message_reply_markup(
        chat_id=user_id,
        message_id=callback.message.message_id,
        reply_markup=None
    )
    
    opts = question_data.get('options', [])
    
    if answer_index < len(opts):
        selected_answer = opts[answer_index]
        await callback.message.answer(f"Ваш ответ: {selected_answer}")
    else:
        await callback.message.answer("Ошибка: вариант ответа не найден")
        return
    
    if is_correct:
        if user_id not in user_scores:
            user_scores[user_id] = 0
        user_scores[user_id] += 1
        await callback.message.answer("Верно! +1 очко")
    else:
        correct_index = question_data.get('correct_option', 0)
        if correct_index < len(opts):
            correct_answer = opts[correct_index]
            await callback.message.answer(f"Неправильно. Правильный ответ: {correct_answer}")
    
    current_question_index += 1
    await update_quiz_index(user_id, current_question_index)
    
    if current_question_index < len(quiz_data):
        await get_question(callback.message, user_id)
    else:
        score = user_scores.get(user_id, 0)
        username = callback.from_user.username or callback.from_user.first_name or "Аноним"
        
        await save_quiz_result(user_id, username, score)
        
        import asyncio
        await asyncio.sleep(1)
        
        user_stats = await get_user_stats(user_id)
        
        if user_stats:
            last_score, total_played, last_played, all_score = user_stats
        else:
            last_score = score
            total_played = 1
            all_score = score
            
        if user_id in user_quiz_data:
            del user_quiz_data[user_id]
        if user_id in user_scores:
            del user_scores[user_id]
                      
        await callback.message.answer(
            f"Квиз завершен!\n"
            f"Результат этой игры: {score} из {len(quiz_data)} очков\n"
            f"Общий счет (все игры): {all_score} очков\n"
            f"Всего сыграно игр: {total_played}\n\n"
            f"Используйте ""Статистика"" чтобы посмотреть полную статистику",
            reply_markup=get_main_keyboard()
        )

@router.message(Command("stats"))
@router.message(F.text == "Статистика")
async def cmd_stats(message: types.Message):
    user_stats = await get_user_stats(message.from_user.id)
    
    if user_stats:
        last_score, total_played, last_played, all_score = user_stats
        await message.answer(
            f"Ваша статистика:\n"
            f"Последний результат: {last_score} очков\n"
            f"Всего сыграно: {total_played} раз\n"
            f"Общий счет: {all_score} очков\n"
            f"Последняя игра: {last_played.split()[0] if last_played else 'Неизвестно'}"
        )
    else:
        await message.answer("Вы еще не играли в квиз! Начните игру с помощью кнопки 'Начать игру'")
        
@router.message(Command("leaderboard"))
@router.message(F.text == "Таблица лидеров")
async def cmd_leaderboard(message: types.Message):
    all_stats = await get_all_stats()
    
    if not all_stats:
        await message.answer("Пока нет статистики игроков")
        return
    
    leaderboard_text = "Таблица лидеров (по общему счету):\n\n"
    for i, (username, last_score, total_played, last_played, all_score) in enumerate(all_stats[:10], 1):
        leaderboard_text += f"{i}. {username or 'Без имени'}: {all_score} очков (игр: {total_played})\n"
    
    await message.answer(leaderboard_text)