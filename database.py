import os
import ydb
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

_pool = None

def get_ydb_pool():
    global _pool
    if _pool is None:
        ydb_endpoint = os.getenv("YDB_ENDPOINT")
        ydb_database = os.getenv("YDB_DATABASE")
        
        if not ydb_endpoint or not ydb_database:
            raise ValueError("YDB_ENDPOINT and YDB_DATABASE environment variables are required")
        
        if not ydb_endpoint.startswith('grpcs://'):
            ydb_endpoint = f"grpcs://{ydb_endpoint}"
        
        ydb_driver_config = ydb.DriverConfig(
            ydb_endpoint,
            ydb_database,
            credentials=ydb.credentials_from_env_variables(),
            root_certificates=ydb.load_ydb_root_certificate(),
        )

        ydb_driver = ydb.Driver(ydb_driver_config)
        ydb_driver.wait(fail_fast=True, timeout=30)
        _pool = ydb.SessionPool(ydb_driver)
    
    return _pool

def _format_kwargs(kwargs):
    return {"${}".format(key): value for key, value in kwargs.items()}

async def execute_update_query(query, **kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, _format_kwargs(kwargs), commit_tx=True
        )
    return get_ydb_pool().retry_operation_sync(callee)

async def execute_select_query(query, **kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, _format_kwargs(kwargs), commit_tx=True
        )
        return result_sets[0].rows if result_sets and len(result_sets) > 0 else []
    
    return get_ydb_pool().retry_operation_sync(callee)

async def get_quiz_index(user_id):
    get_user_index = """
        DECLARE $user_id AS Uint64;
        SELECT question_index FROM quiz_state WHERE user_id = $user_id;
    """
    results = await execute_select_query(get_user_index, user_id=user_id)
    
    if not results or len(results) == 0:
        return 0
    
    return results[0].get("question_index", 0)

async def update_quiz_index(user_id, index):
    set_quiz_state = """
        DECLARE $user_id AS Uint64;
        DECLARE $question_index AS Uint32;
        UPSERT INTO quiz_state (user_id, question_index) VALUES ($user_id, $question_index);
    """
    await execute_update_query(set_quiz_state, user_id=user_id, question_index=index)

async def save_quiz_result(user_id, username, score):
    current_timestamp = datetime.now()
    
    get_current_stats = """
        DECLARE $user_id AS Uint64;
        SELECT total_played, all_score FROM user_stats WHERE user_id = $user_id;
    """
    results = await execute_select_query(get_current_stats, user_id=user_id)
    
    upsert_stats = """
        DECLARE $user_id AS Uint64;
        DECLARE $username AS Utf8;
        DECLARE $last_score AS Uint32;
        DECLARE $total_played AS Uint32;
        DECLARE $all_score AS Uint64;
        DECLARE $last_played AS Timestamp;
        
        UPSERT INTO user_stats (user_id, username, last_score, total_played, all_score, last_played)
        VALUES ($user_id, $username, $last_score, $total_played, $all_score, $last_played);
    """
    
    if results and len(results) > 0:
        current = results[0]
        total_played = current.get("total_played", 0) + 1
        all_score = current.get("all_score", 0) + score
        
        await execute_update_query(
            upsert_stats,
            user_id=user_id,
            username=username or "",
            last_score=score,
            total_played=total_played,
            all_score=all_score,
            last_played=current_timestamp
        )
    else:
        await execute_update_query(
            upsert_stats,
            user_id=user_id,
            username=username or "",
            last_score=score,
            total_played=1,
            all_score=score,
            last_played=current_timestamp
        )
        
async def get_user_stats(user_id):
    get_stats = """
        DECLARE $user_id AS Uint64;
        SELECT last_score, total_played, last_played, all_score
        FROM user_stats WHERE user_id = $user_id;
    """
    results = await execute_select_query(get_stats, user_id=user_id)
    
    if not results or len(results) == 0:
        return None
    
    result = results[0]
    last_played = result.get("last_played")
    
    if isinstance(last_played, int):
        last_played_seconds = last_played / 1000000
        last_played_dt = datetime.fromtimestamp(last_played_seconds)
        last_played_str = last_played_dt.strftime("%Y-%m-%d %H:%M:%S")
    elif hasattr(last_played, 'strftime'):
        last_played_str = last_played.strftime("%Y-%m-%d %H:%M:%S")
    else:
        last_played_str = str(last_played) if last_played else "Неизвестно"
    
    return (
        result.get("last_score", 0),
        result.get("total_played", 0),
        last_played_str,
        result.get("all_score", 0)
    )

async def get_all_stats():
    get_all_stats_query = """
        SELECT username, last_score, total_played, last_played, all_score
        FROM user_stats 
        ORDER BY all_score DESC 
        LIMIT 10;
    """
    results = await execute_select_query(get_all_stats_query)
    
    if not results:
        return []
    
    formatted_results = []
    for result in results:
        last_played = result.get("last_played")
        
        if hasattr(last_played, 'strftime'):
            last_played_str = last_played.strftime("%Y-%m-%d %H:%M:%S")
        elif last_played:
            last_played_str = str(last_played)
        else:
            last_played_str = "Неизвестно"
            
        formatted_results.append((
            result.get("username", ""),
            result.get("last_score", 0),
            result.get("total_played", 0),
            last_played_str,
            result.get("all_score", 0)
        ))
    
    return formatted_results
        
async def get_quiz_data():
    get_all_ids = "SELECT question_id FROM quiz_data;"
    all_ids_results = await execute_select_query(get_all_ids)
    
    if not all_ids_results:
        return []
    
    import random
    all_ids = [row['question_id'] for row in all_ids_results]
    
    if len(all_ids) < 10:
        selected_ids = all_ids
    else:
        selected_ids = random.sample(all_ids, 10)
    
    if selected_ids:
        placeholders = ', '.join([str(id) for id in selected_ids])
        get_questions = f"SELECT question_id, question, options, correct_option FROM quiz_data WHERE question_id IN ({placeholders})"
        results = await execute_select_query(get_questions)
    else:
        results = []
    
    quiz_data = []
    for result in results:
        options_json = result.get("options", "[]")
        try:
            options_list = json.loads(options_json)
        except:
            options_list = []
            
        quiz_data.append({
            'question_id': result.get("question_id"),
            'question': result.get("question", ""),
            'options': options_list,
            'correct_option': result.get("correct_option", 0)
        })
    
    return quiz_data

async def get_quiz_question(question_index, quiz_data):
    if question_index is None or question_index < 0 or question_index >= len(quiz_data):
        return None
        
    question_data = quiz_data[question_index]
    
    if not question_data.get("question") or not question_data.get("options"):
        return None
        
    return question_data