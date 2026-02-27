import pandas as pd
import mysql.connector
import re
import phpserialize
import json
import logging
from bs4 import BeautifulSoup
import os

# ==========================
# LOGGING
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import_log.txt'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================
# DB CONFIG (can be overridden by environment variables)
# ==========================
DB_HOST = os.environ.get('DB_HOST', '31.97.62.2')
DB_USER = os.environ.get('DB_USER', 'simuser')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'simpoint@2026')
DB_NAME = os.environ.get('DB_NAME', 'simpoint_db')
PORT = int(os.environ.get('DB_PORT', 3306))

# Batch commit every N rows to avoid long transactions and connection timeout
BATCH_SIZE = 500
# Max retries on lost connection (error 2013)
MAX_RECONNECT_RETRIES = 3


def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=PORT,
        auth_plugin='mysql_native_password',
        charset='utf8mb4',
        connection_timeout=60,
        autocommit=False
    )


# ==========================
# HELPERS
# ==========================
def clean(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except:
        pass
    if isinstance(v, str) and v.strip().lower() in ("nan", "null", "none", ""):
        return None
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


def is_json(val):
    if not isinstance(val, str):
        return False
    try:
        json.loads(val)
        return True
    except:
        return False


def get_table_columns(cursor, table):
    cursor.execute(f"SHOW COLUMNS FROM `{table}`")
    return {row[0] for row in cursor.fetchall()}


def get_id_set(cursor, table, column):
    cursor.execute(f"SELECT `{column}` FROM `{table}`")
    return {row[0] for row in cursor.fetchall()}


# ==========================
# CONTENT → BLOCK JSON
# ==========================
def strip_gutenberg(html):
    if not html:
        return None
    html = re.sub(r'<!--.*?-->', '', str(html), flags=re.DOTALL)
    return html.strip()


def _children_with_links(tag):
    """Build children list preserving <a> links (url + text) and plain text."""
    children = []
    for node in tag.children:
        if getattr(node, "name", None) == "a":
            href = node.get("href") or ""
            text = node.get_text(strip=True)
            children.append({"type": "link", "url": href, "text": text})
        elif hasattr(node, "get_text"):
            text = node.get_text(strip=True)
            if text:
                children.append({"type": "text", "text": text})
        else:
            text = str(node).strip()
            if text:
                children.append({"type": "text", "text": text})
    if not children:
        children = [{"type": "text", "text": tag.get_text(strip=True) or ""}]
    return children


def html_to_block_json(html):
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    blocks = []

    for tag in soup.find_all(["h1", "h2", "h3", "p", "ul", "ol"]):
        if tag.name.startswith("h"):
            blocks.append({
                "type": "heading",
                "level": int(tag.name[1]),
                "children": _children_with_links(tag)
            })
        elif tag.name == "p":
            blocks.append({
                "type": "paragraph",
                "children": _children_with_links(tag)
            })
        elif tag.name in ("ul", "ol"):
            blocks.append({
                "type": "list",
                "ordered": tag.name == "ol",
                "items": [{"text": li.get_text(strip=True)} for li in tag.find_all("li")]
            })

    return {"content": blocks} if blocks else None


def plain_text_to_block_json(text):
    if not text:
        return {"content": []}
    return {
        "content": [
            {"type": "paragraph", "children": [{"type": "text", "text": str(text).strip()}]}
        ]
    }


def content_to_json(content):
    if not content:
        return {"content": []}
    if is_json(content):
        try:
            return json.loads(content)
        except:
            return plain_text_to_block_json(content)
    block = html_to_block_json(strip_gutenberg(content))
    return block if block else plain_text_to_block_json(content)


# ==========================
# PHP SERIALIZED → MCQ JSON
# ==========================
def _php_object_hook(classname, props):
    return props


def safe_bool(v):
    try:
        return bool(int(v))
    except:
        return bool(v)


def safe_int(v):
    try:
        return int(float(v))
    except:
        return 0


def _regex_fallback_answer_data(s):
    """When phpserialize fails, extract *_answer, *_correct, *_points from raw PHP serialized string."""
    s = str(s)
    answer_texts = []
    for m in re.finditer(r'"\*_answer";s:(\d+):"', s):
        start = m.end()
        length = int(m.group(1))
        if start + length <= len(s):
            text = s[start:start + length]
            if text.strip():
                answer_texts.append(text.strip())
    correct_list = [bool(int(c)) for c in re.findall(r'"\*_correct";[bi]:(\d+)', s)]
    points_list = []
    for m in re.finditer(r'"\*_points";(?:i:(\d+)|d:([\d.]+)|N);', s):
        if m.group(1) is not None:
            points_list.append(int(m.group(1)))
        elif m.group(2) is not None:
            points_list.append(int(float(m.group(2))))
        else:
            points_list.append(0)
    n = len(answer_texts)
    if n == 0:
        return None
    correct_list.extend([False] * (n - len(correct_list)))
    points_list.extend([0] * (n - len(points_list)))
    total_points = 0
    options = []
    for i in range(n):
        correct = correct_list[i] if i < len(correct_list) else False
        pts = points_list[i] if i < len(points_list) else 0
        if correct and pts == 0:
            pts = 1
        total_points += pts
        options.append({
            "id": i + 1,
            "text": answer_texts[i],
            "correct": correct,
            "points": pts
        })
    return {"questionType": "MCQ", "totalPoints": total_points, "options": options}


def convert_answer_data(val):
    base = {
        "questionType": "MCQ",
        "totalPoints": 0,
        "options": []
    }

    if not val or not str(val).strip():
        return base

    s = str(val).strip()
    if is_json(s):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and "options" in obj and isinstance(obj["options"], list):
                return obj
        except Exception:
            pass

    try:
        cleaned = s.replace("N;", "s:0:\"\";")
        parsed = phpserialize.loads(
            cleaned.encode("utf-8", "ignore"),
            decode_strings=True,
            object_hook=_php_object_hook
        )

        if not isinstance(parsed, dict):
            fallback = _regex_fallback_answer_data(s)
            return fallback if fallback else base

        option_id = 1
        for obj in parsed.values():
            if not isinstance(obj, dict):
                continue
            text = obj.get("*_answer")
            if not isinstance(text, str) or not text.strip():
                continue
            is_correct = bool(obj.get("*_correct", False))
            points = obj.get("*_points", 0)
            if isinstance(points, float):
                points = int(points)
            if is_correct and points == 0:
                points = 1
            base["options"].append({
                "id": option_id,
                "text": text.strip(),
                "correct": is_correct,
                "points": points
            })
            base["totalPoints"] += points
            option_id += 1

        if base["options"]:
            return base
        fallback = _regex_fallback_answer_data(s)
        return fallback if fallback else base

    except Exception:
        fallback = _regex_fallback_answer_data(s)
        if fallback and fallback.get("options"):
            return fallback
        return base


# ==========================
# UPSERT
# ==========================
def build_upsert_query(table, columns):
    quoted = [f"`{c}`" for c in columns]
    updates = [f"`{c}`=VALUES(`{c}`)" for c in columns if c.lower() != "id"]
    return f"""
        INSERT INTO `{table}` ({",".join(quoted)})
        VALUES ({",".join(["%s"] * len(columns))})
        ON DUPLICATE KEY UPDATE {",".join(updates)}
    """


# ==========================
# SHEET ORDER
# ==========================
sheet_order = [
    'editors',
    'categories',
    'subcategories',
    'topic_categories',
    'courses',
    'modules',
    'lessons',
    'assessments',
    'module_contents',
    'questions',
    'question_links'
]


def import_excel(excel_path):
    """Main entry point used by the Flask UI.  Takes a path to an Excel file and
    imports its sheets into the configured MySQL/MariaDB database."""
    conn = get_connection()
    cursor = conn.cursor()
    logger.info("Connected to MariaDB")

    xls = pd.ExcelFile(excel_path)
    logger.info(f"Sheets found: {xls.sheet_names}")

    for sheet in sheet_order:
        if sheet not in xls.sheet_names:
            continue

        logger.info(f"Processing {sheet}")
        df = pd.read_excel(excel_path, sheet_name=sheet)

        # Lessons: use Excel lesson ID as lesson_id (add UNIQUE on lesson_id in DB to avoid duplicate rows on re-run)
        if sheet == "lessons":
            if "lesson_id" not in df.columns:
                renamed = False
                for cand in ["ID", "Id", "Lesson ID", "lessonID", "lessonId", "LessonId", "LESSON_ID", "Lesson_ID"]:
                    if cand in df.columns:
                        df = df.rename(columns={cand: "lesson_id"})
                        renamed = True
                        break
                if not renamed:
                    for c in df.columns:
                        if str(c).strip().lower() in ("lessonid", "lesson_id", "lesson id"):
                            df = df.rename(columns={c: "lesson_id"})
                            break
            if "lesson_id" in df.columns:
                df["lesson_id"] = df["lesson_id"].map(clean)

        # module_contents: ensure Excel lessonId/Lesson ID -> lesson_id so column is not dropped
        if sheet == "module_contents" and "lesson_id" not in df.columns:
            for cand in ["lessonId", "LessonId", "Lesson ID", "lessonID", "LESSON_ID", "Lesson_ID"]:
                if cand in df.columns:
                    df = df.rename(columns={cand: "lesson_id"})
                    break
            else:
                for c in list(df.columns):
                    if str(c).strip().lower() in ("lessonid", "lesson_id", "lesson id"):
                        df = df.rename(columns={c: "lesson_id"})
                        break

        db_cols = get_table_columns(cursor, sheet)
        df = df[[c for c in df.columns if c in db_cols]]

        if df.empty:
            continue

        # Only convert HTML/JSON content columns; never transform link/URL columns (keep as-is)
        link_like = {"link", "url", "video_url", "video_link", "lesson_link", "content_link"}
        for col in [
            "course_description",
            "lesson_content",
            "assessment_content",
            "module_description",
            "question_content"
        ]:
            if col in df.columns and col.lower() not in link_like:
                df[col] = df[col].map(lambda x: json.dumps(content_to_json(x), ensure_ascii=False))

        if sheet == "questions" and "answer_data" in df.columns:
            df["answer_data"] = df["answer_data"].map(
                lambda x: json.dumps(convert_answer_data(x), ensure_ascii=False)
            )

        # questions: convert correct_msg and incorrect_msg to JSON format (block content like question_content)
        if sheet == "questions":
            for col in ("correct_msg", "incorrect_msg"):
                if col in df.columns:
                    df[col] = df[col].map(lambda x: json.dumps(content_to_json(x), ensure_ascii=False))

        # Ensure foreign keys are safe before insert
        if sheet == "module_contents" and "lesson_id" in df.columns:
            lesson_ids = get_id_set(cursor, "lessons", "lesson_id")
            def fix_lesson_id(v):
                if v is None:
                    return None
                try:
                    if isinstance(v, float) and (v != v or pd.isna(v)):
                        return None
                    lid = int(float(v))
                    return lid if lid in lesson_ids else None
                except (ValueError, TypeError):
                    return None
            df["lesson_id"] = df["lesson_id"].map(fix_lesson_id)

        if sheet == "module_contents" and "module_id" in df.columns:
            # Excel may have different module_ids in modules vs module_contents — map by position
            mod_df = pd.read_excel(excel_path, sheet_name="modules")
            if "module_id" in mod_df.columns:
                mod_ids = mod_df["module_id"].drop_duplicates().tolist()
                mc_ids = df["module_id"].drop_duplicates().tolist()
                mc_to_mod = dict(zip(mc_ids[: len(mod_ids)], mod_ids[: len(mc_ids)]))
                def map_module_id(v):
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        return None
                    try:
                        mid = int(float(v))
                        return mc_to_mod.get(mid, mid)
                    except (ValueError, TypeError):
                        return None
                df["module_id"] = df["module_id"].map(map_module_id)
            module_ids = get_id_set(cursor, "modules", "module_id")
            def fix_module_id(v):
                if v is None:
                    return None
                try:
                    if isinstance(v, float) and (v != v or pd.isna(v)):
                        return None
                    mid = int(float(v))
                    return mid if mid in module_ids else None
                except (ValueError, TypeError):
                    return None
            df["module_id"] = df["module_id"].map(fix_module_id)
            df = df[df["module_id"].notna()]
            if df.empty:
                logger.info(f"{sheet}: no rows with valid module_id after filtering; skipping")
                continue

        if sheet == "module_contents" and "assessment_id" in df.columns:
            assessment_ids = get_id_set(cursor, "assessments", "assessment_id")
            def fix_assessment_id(v):
                if v is None:
                    return None
                try:
                    if isinstance(v, float) and (v != v or pd.isna(v)):
                        return None
                    aid = int(float(v))
                    return aid if aid in assessment_ids else None
                except (ValueError, TypeError):
                    return None
            df["assessment_id"] = df["assessment_id"].map(fix_assessment_id)

        def fix_created_by_col(editor_ids, series):
            def fix(v):
                if v is None:
                    return None
                try:
                    if isinstance(v, float) and (v != v or pd.isna(v)):
                        return None
                    vid = int(float(v))
                    return vid if vid in editor_ids else None
                except (ValueError, TypeError):
                    return None
            return series.map(fix)

        if sheet == "questions" and "created_by" in df.columns:
            editor_ids = get_id_set(cursor, "editors", "editor_id")
            df["created_by"] = fix_created_by_col(editor_ids, df["created_by"])

        if sheet == "lessons" and "created_by" in df.columns:
            editor_ids = get_id_set(cursor, "editors", "editor_id")
            df["created_by"] = fix_created_by_col(editor_ids, df["created_by"])

        if sheet == "assessments":
            if "last_update" in df.columns:
                now = pd.Timestamp.now().normalize()
                df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce").fillna(now)
            if "created_by" in df.columns:
                editor_ids = get_id_set(cursor, "editors", "editor_id")
                df["created_by"] = fix_created_by_col(editor_ids, df["created_by"])

        if sheet in ("lessons", "assessments") and "status" in df.columns:
            def norm_status(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return "draft"
                s = str(v).strip().lower()
                if s in ("", "nan", "null", "none"):
                    return "draft"
                if s in ("1", "published", "active", "yes"):
                    return "published"
                if s in ("0", "draft", "inactive", "no"):
                    return "draft"
                return s if s in ("draft", "published") else "draft"
            df["status"] = df["status"].map(norm_status)

        if sheet == "modules" and "course_id" in df.columns:
            course_ids = get_id_set(cursor, "courses", "course_id")
            def fix_module_course_id(v):
                if v is None:
                    return None
                try:
                    if isinstance(v, float) and (v != v or pd.isna(v)):
                        return None
                    cid = int(float(v))
                    return cid if cid in course_ids else None
                except (ValueError, TypeError):
                    return None
            df["course_id"] = df["course_id"].map(fix_module_course_id)
            df = df[df["course_id"].notna()]
            if df.empty:
                logger.info(f"{sheet}: no rows with valid course_id after filtering; skipping")
                continue

        if sheet == "courses":
            now = pd.Timestamp.now().normalize()
            if "publish_date" in df.columns:
                df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce").fillna(now)
            if "last_update" in df.columns:
                df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce").fillna(now)

        if sheet == "question_links":
            if "assessment_id" in df.columns:
                assessment_ids = get_id_set(cursor, "assessments", "assessment_id")
                def fix_ql_assessment_id(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, float) and (v != v or pd.isna(v)):
                            return None
                        aid = int(float(v))
                        return aid if aid in assessment_ids else None
                    except (ValueError, TypeError):
                        return None
                df["assessment_id"] = df["assessment_id"].map(fix_ql_assessment_id)
                df = df[df["assessment_id"].notna()]
            if "question_id" in df.columns:
                question_ids = get_id_set(cursor, "questions", "question_id")
                def fix_ql_question_id(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, float) and (v != v or pd.isna(v)):
                            return None
                        qid = int(float(v))
                        return qid if qid in question_ids else None
                    except (ValueError, TypeError):
                        return None
                df["question_id"] = df["question_id"].map(fix_ql_question_id)
                df = df[df["question_id"].notna()]
            if df.empty:
                logger.info(f"{sheet}: no rows with valid FKs after filtering; skipping")
                continue

        query = build_upsert_query(sheet, list(df.columns))

        for idx, row in enumerate(df.itertuples(index=False, name=None)):
            row_data = tuple(clean(v) for v in row)
            for attempt in range(MAX_RECONNECT_RETRIES):
                try:
                    cursor.execute(query, row_data)
                    break
                except mysql.connector.Error as err:
                    if err.errno == 2013 and attempt < MAX_RECONNECT_RETRIES - 1:
                        logger.warning(f"{sheet} row {idx}: connection lost, reconnecting (attempt {attempt + 1})...")
                        try:
                            cursor.close()
                            conn.close()
                        except Exception:
                            pass
                        conn = get_connection()
                        cursor = conn.cursor()
                    else:
                        logger.error(f"{sheet} row {idx} failed: {err}")
                        break
            if (idx + 1) % BATCH_SIZE == 0:
                try:
                    conn.commit()
                    logger.info(f"{sheet}: committed batch up to row {idx + 1}")
                except mysql.connector.Error as err:
                    logger.error(f"{sheet} batch commit failed: {err}")

        try:
            conn.commit()
        except mysql.connector.Error as err:
            logger.error(f"{sheet} final commit failed: {err}")
        logger.info(f"{sheet} imported successfully")

    # cleanup after all sheets
    cursor.close()
    conn.close()
    logger.info("IMPORT COMPLETED SUCCESSFULLY")

