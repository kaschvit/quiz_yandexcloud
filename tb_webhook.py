import os
import json
import logging
from aiogram import Bot, Dispatcher, types
import handlers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TOKEN = os.getenv("API_TOKEN")

if not API_TOKEN:
    logger.error("API_TOKEN environment variable is not set")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
dp.include_router(handlers.router)

_db_initialized = False

async def ensure_db_initialized():
    global _db_initialized
    if not _db_initialized:
        try:
            await handlers.load_quiz_data()
            _db_initialized = True
        except Exception as e:
            logger.error(f"Error initializing database: {e}")

async def process_event(event):
    try:
        await ensure_db_initialized()
        
        body = event.get('body', '{}')
        
        try:
            data = json.loads(body)
            
            if 'update_id' in data:
                update = types.Update.model_validate(data)
                await dp.feed_update(bot, update)
                
        except json.JSONDecodeError:
            pass
            
    except Exception as e:
        logger.error(f"Error processing event: {e}")

async def webhook(event, context):
    try:
        if event is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Bad request'})
            }
        
        http_method = event.get('httpMethod')
        
        if http_method == 'POST':
            await process_event(event)
            
            return {
                'statusCode': 200,
                'body': 'ok'
            }
        else:
            return {
                'statusCode': 405,
                'body': 'Method not allowed'
            }
            
    except Exception as e:
        logger.error(f"Error in webhook: {e}")
        return {
            'statusCode': 200,
            'body': 'ok'
        }