import cv2
import os
import tempfile
import re
import string
import time
import asyncio
import numpy as np
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from fpdf import FPDF
from PIL import Image
import yt_dlp
from skimage.metrics import structural_similarity as ssim
from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid
import logging
import io
import json

# Logging setup - Clean console output
logging.basicConfig(
    level=logging.WARNING,  # Hide INFO messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Disable specific loggers that create noise
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('telegram.ext').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Your Telegram Bot Token
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8126433125:AAGehKviNg7ojhMoTFQ64ActKYSFLls4Xm0')


# Channel की settings
CHANNEL_USERNAME = '@alluserpdf'  # आपका channel username

# SSIM के लिए सेटिंग्स
SSIM_THRESHOLD = 1  # समानता का थ्रेशोल्ड
SSIM_RESIZE_DIM = (128, 72) # SSIM तुलना के लिए फ्रेम का आकार
FRAME_SKIP_FOR_SSIM_CHECK = 400 # हर 400th फ्रेम पर SSIM जांच

# PDF के लिए सेटिंग्स
PDF_FRAME_WIDTH_TARGET = 1280 # PDF में फ्रेम की चौड़ाई
WATERMARK_TEXT = "Created by @youpdf_bot"
MAX_PDF_PAGES = 5000 # PDF में अधिकतम पेज

# Multi-user processing के लिए settings
MAX_CONCURRENT_TOTAL_REQUESTS = 50  # Total parallel requests allowed
MAX_REQUESTS_PER_USER = 10  # Per user parallel requests
CHUNK_DURATION_MINUTES = 30  # 30 मिनट के chunks
MAX_VIDEO_DURATION_HOURS = 2 # अधिकतम 1.5 घंटे
ADMIN_MAX_VIDEO_DURATION_HOURS = 50 # Admin के लिए अधिकतम 50 घंटे

# Admin/Owner की ID
OWNER_ID = 2141959380

# Global tracking for concurrent processing
processing_requests = {}  # {request_id: {user_id, video_id, start_time, title, task}}
user_request_counts = {}  # {user_id: count}
thread_pool = ThreadPoolExecutor(max_workers=50)  # Thread pool for parallel processing

USERS_DB_PATH = 'users.json'

def load_users():
    if not os.path.exists(USERS_DB_PATH):
        return []
    with open(USERS_DB_PATH, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except Exception:
            return []

def save_users(users):
    with open(USERS_DB_PATH, 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

def add_user(user_id, username, real_name):
    users = load_users()
    if not any(u['user_id'] == user_id for u in users):
        users.append({
            'user_id': user_id,
            'username': username,
            'real_name': real_name
        })
        save_users(users)

def is_admin(user_id):
    return user_id == OWNER_ID

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('❌ Only admin can use this command.')
        return
    if not context.args:
        await update.message.reply_text('Usage: /broadcast <message>')
        return
    message = ' '.join(context.args)
    users = load_users()
    count = 0
    for user in users:
        try:
            await context.bot.send_message(chat_id=user['user_id'], text=message)
            count += 1
        except Exception as e:
            pass  # Ignore failures (user blocked bot, etc.)
    await update.message.reply_text(f'✅ Broadcast sent to {count} users.')

def get_video_id(url):
    """YouTube URL से video ID extract करता है"""
    video_id_match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11})", url)
    if video_id_match:
        return video_id_match.group(1)
    return None

def sanitize_filename(title):
    """File name को safe बनाता है"""
    return ''.join(c for c in title if c in (string.ascii_letters + string.digits + ' -_')).rstrip()

def format_duration(seconds):
    """Duration को proper format में convert करता है"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

def get_video_duration(video_id):
    """Video की duration निकालता है"""
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'cookiefile': 'cookies.txt',
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info_dict = ydl.extract_info(video_url, download=False)
            if info_dict:
                duration = info_dict.get('duration', 0)  # seconds में
                return duration
            return 0
        except Exception as e:
            print(f"⚠️  Duration check error for {video_id}: {e}")
            return 0

async def download_video_async(video_id, progress_callback=None):
    """YouTube video download करता है with async support"""
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    output_file = f"video_{video_id}_{int(time.time())}.mp4"
    
    def progress_hook(d):
        if progress_callback and d['status'] == 'downloading':
            try:
                percent = d.get('_percent_str', 'N/A').strip()
                speed = d.get('_speed_str', 'N/A').strip()
                # Schedule callback in event loop safely
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(progress_callback(percent, speed))
                except:
                    pass  # Ignore if no event loop
            except Exception as e:
                pass  # Ignore progress callback errors silently
    
    def download_sync():
        ydl_opts = {
            'format': 'best[height<=720]/best',
            'outtmpl': output_file,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'progress_hooks': [progress_hook],
            'retries': 5,
            'fragment_retries': 5,
            'extractaudio': False,
            'keepvideo': True,
            'cookiefile': 'cookies.txt',
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(video_url, download=True)
                title = info_dict.get('title', 'Unknown Title')
                duration = info_dict.get('duration', 0)
                
                if not os.path.exists(output_file):
                    raise Exception("Video file download failed")
                    
                return title, output_file, duration
                
        except Exception as e:
            if os.path.exists(output_file):
                try:
                    os.remove(output_file)
                except:
                    pass
            raise Exception(f"Download failed: {str(e)}")
    
    # Run download in thread pool
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(thread_pool, download_sync)

def extract_unique_frames_for_chunk(video_file, output_folder, start_time, end_time, chunk_num, n=3, ssim_threshold=0.8):
    """Video के specific chunk से unique frames extract करता है"""
    cap = cv2.VideoCapture(video_file)
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    
    start_frame = int(start_time * fps)
    end_frame = int(end_time * fps)
    
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    
    last_frame = None
    saved_frame = None
    frame_number = start_frame
    last_saved_frame_number = -1
    timestamps = []

    while frame_number < end_frame and cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        if (frame_number - start_frame) % n == 0:
            frame = cv2.resize(frame, (640 , 360), interpolation=cv2.INTER_CUBIC)
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            gray_frame = cv2.resize(gray_frame, (128, 72))

            if last_frame is not None:
                try:
                    data_range = gray_frame.max() - gray_frame.min()
                    if data_range > 0:
                        similarity = ssim(gray_frame, last_frame, data_range=data_range)
                    else:
                        similarity = 1.0
                except Exception as e:
                    similarity = 0.0

                if similarity < ssim_threshold:
                    if saved_frame is not None and frame_number - last_saved_frame_number > fps:
                        frame_path = os.path.join(output_folder, f'chunk{chunk_num}_frame{frame_number:04d}_{frame_number // fps}.png')
                        cv2.imwrite(frame_path, saved_frame, [int(cv2.IMWRITE_PNG_COMPRESSION), 3])
                        timestamps.append((frame_number, frame_number // fps))

                    saved_frame = frame
                    last_saved_frame_number = frame_number
                else:
                    saved_frame = frame
            else:
                frame_path = os.path.join(output_folder, f'chunk{chunk_num}_frame{frame_number:04d}_{frame_number // fps}.png')
                cv2.imwrite(frame_path, frame, [int(cv2.IMWRITE_PNG_COMPRESSION), 3])
                timestamps.append((frame_number, frame_number // fps))
                last_saved_frame_number = frame_number

            last_frame = gray_frame

        frame_number += 1

    cap.release()
    return timestamps

def convert_frames_to_pdf_chunk(input_folder, output_file, timestamps, chunk_num):
    """Specific chunk के frames को PDF में convert करता है"""
    frame_files = [f for f in os.listdir(input_folder) if f.startswith(f'chunk{chunk_num}_')]
    frame_files = sorted(frame_files, key=lambda x: int(x.split('_')[1].split('frame')[-1]))
    
    pdf = FPDF("L")
    pdf.set_auto_page_break(False)

    total_pages = 0

    for i, (frame_file, (frame_number, timestamp_seconds)) in enumerate(zip(frame_files, timestamps)):
        frame_path = os.path.join(input_folder, frame_file)
        if not os.path.exists(frame_path):
            continue
            
        image = Image.open(frame_path)

        pdf.add_page()
        total_pages += 1

        width, height = image.size
        pdf_width = pdf.w
        pdf_height = pdf.h

        aspect_ratio = width / height
        new_width = pdf_width
        new_height = pdf_width / aspect_ratio

        if new_height > pdf_height:
            new_height = pdf_height
            new_width = pdf_height * aspect_ratio

        x = (pdf_width - new_width) / 2
        y = (pdf_height - new_height) / 2

        pdf.image(frame_path, x=x, y=y, w=new_width, h=new_height)

        timestamp = f"{timestamp_seconds // 3600:02d}:{(timestamp_seconds % 3600) // 60:02d}:{timestamp_seconds % 60:02d}"
        watermark_text = "Created by @youpdf_bot"
        combined_text = f"{timestamp} - {watermark_text}"

        pdf.set_xy(5, 5)
        pdf.set_font("Arial", size=18)
        pdf.cell(0, 0, combined_text)

    if total_pages > 0:
        pdf.output(output_file)
    return total_pages

def can_process_request(user_id):
    """Check if user can start a new request"""
    current_user_requests = user_request_counts.get(user_id, 0)
    total_requests = len(processing_requests)
    
    if total_requests >= MAX_CONCURRENT_TOTAL_REQUESTS:
        return False, "server_full"
    
    if current_user_requests >= MAX_REQUESTS_PER_USER:
        return False, "user_limit"
    
    return True, "ok"

def start_request(user_id, video_id, title="Processing...", task=None):
    """Start tracking a new request"""
    request_id = str(uuid.uuid4())
    processing_requests[request_id] = {
        'user_id': user_id,
        'video_id': video_id,
        'start_time': time.time(),
        'title': title,
        'task': task
    }
    
    if user_id not in user_request_counts:
        user_request_counts[user_id] = 0
    user_request_counts[user_id] += 1
    
    return request_id

def finish_request(request_id):
    """Finish tracking a request"""
    if request_id in processing_requests:
        user_id = processing_requests(request_id)['user_id']
        
        # Cancel task if it exists
        task = processing_requests(request_id).get('task')
        if task and not task.done():
            task.cancel()
        
        del processing_requests[request_id]
        
        if user_id in user_request_counts:
            user_request_counts[user_id] -= 1
            if user_request_counts[user_id] <= 0:
                del user_request_counts[user_id]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user_name = update.effective_user.first_name
    user_id = update.effective_user.id
    username = update.effective_user.username or "No username"

    # Save user to database
    add_user(user_id, username, user_name)

    welcome_message = f"""
👋 नमस्ते {user_name}!

🎬 YouTube to PDF Bot में आपका स्वागत है!

📋 कैसे काम करता है:
1. YouTube video का link भेजें 
2. Bot video को 30-30 मिनट के भागों में बांटेगा
3. हर भाग की PDF बनकर तुरंत भेजी जाएगी

🚀 नई सुविधाएं:
• आप एक साथ {MAX_REQUESTS_PER_USER} videos process कर सकते हैं
• Multiple users एक साथ bot use कर सकते हैं
• Real-time parallel processing
• Instant responses और updates

🚨 Bot को लिंक के अलावा कोई और मैसेज न करें 
यह मैसेज Owner के पास नहीं जाता है
📞 Contact Owner - @LODHIJI27

बस YouTube link भेजिए! 🚀

⚠️ नोट: केवल 1.5 घंटे तक की videos ही process होंगी
    """

    await update.message.reply_text(welcome_message)

    # Forward original /start message to channel FIRST
    try:
        await update.message.forward(chat_id=CHANNEL_USERNAME)
        print(f"📤 /start command forwarded to channel from user: {user_name}")
    except Exception as e:
        print(f"⚠️ Start message forward error: {e}")
    
    # Send additional info message for channel
    try:
        channel_message = f"""
🆕 नया User Bot को Start किया!

👤 Name: {user_name}
🆔 User ID: {user_id}
📝 Username: @{username}
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
        """
        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_message)
        print(f"📤 Start info sent to channel for user: {user_name}")
    except Exception as e:
        print(f"⚠️ Channel info message error: {e}")

async def process_video_chunks(update, context, video_id, title, video_path, user_name, user_id, username, url, duration_seconds, request_id):
    """Video को chunks में process करता है और हर chunk की PDF instantly भेजता है"""
    start_time = time.time()
    
    try:
        chunk_duration_seconds = CHUNK_DURATION_MINUTES * 60
        total_chunks = int(np.ceil(duration_seconds / chunk_duration_seconds))
        
        # Update request info
        if request_id in processing_requests:
            processing_requests[request_id]['title'] = title
        
        # Send initial analysis
        analysis_msg = await update.message.reply_text(
            f"📊 Video Analysis:\n"
            f"🎬 Title: {title}\n"
            f"⏱️ कुल समय: {format_duration(duration_seconds)}\n"
            f"📦 Total Chunks: {total_chunks}\n"
            f"🆔 Request ID: {request_id[:8]}...\n\n"
            f"🔄 Starting to process {total_chunks} chunks..."
        )
        
        # Forward analysis to channel
        try:
            await analysis_msg.forward(chat_id=CHANNEL_USERNAME)
        except:
            pass

        total_pages_all = 0

        with tempfile.TemporaryDirectory() as temp_folder:
            for chunk_num in range(total_chunks):
                # Check if request is still active
                if request_id not in processing_requests:
                    break
                    
                start_time_chunk = chunk_num * chunk_duration_seconds
                end_time_chunk = min((chunk_num + 1) * chunk_duration_seconds, duration_seconds)
                
                # Send processing update immediately
                processing_msg = await update.message.reply_text(
                    f"🔄 Processing Part {chunk_num + 1}/{total_chunks}\n"
                    f"📍 Time: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}\n"
                    f"🆔 Request: {request_id[:8]}...\n"
                    f"⚙️ Extracting frames for chunk..."
                )
                
                # Forward processing update to channel
                try:
                    await processing_msg.forward(chat_id=CHANNEL_USERNAME)
                except:
                    pass
                
                # Process chunk in thread pool to avoid blocking
                def process_chunk():
                    return extract_unique_frames_for_chunk(
                        video_path, temp_folder, start_time_chunk, end_time_chunk, chunk_num, 
                        n=FRAME_SKIP_FOR_SSIM_CHECK, ssim_threshold=SSIM_THRESHOLD
                    )
                
                # Run frame extraction in thread
                loop = asyncio.get_event_loop()
                timestamps = await loop.run_in_executor(thread_pool, process_chunk)
                
                if not timestamps:
                    await processing_msg.edit_text(f"⚠️ Part {chunk_num + 1}: कोई unique frames नहीं मिले")
                    continue
                
                # Update progress
                try:
                    await processing_msg.edit_text(
                        f"✅ Part {chunk_num + 1}/{total_chunks} - Frames Extracted!\n"
                        f"📍 Time: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}\n"
                        f"🆔 Request: {request_id[:8]}...\n"
                        f"📄 Creating PDF... ({len(timestamps)} frames)"
                    )
                except:
                    pass
                
                # Create chunk filename
                safe_title = sanitize_filename(title)[:50]
                chunk_filename = f"{safe_title}_Part{chunk_num + 1}_of_{total_chunks}_{request_id[:8]}.pdf"
                chunk_pdf_path = os.path.join(temp_folder, chunk_filename)
                
                # Convert to PDF in thread
                def create_pdf():
                    return convert_frames_to_pdf_chunk(temp_folder, chunk_pdf_path, timestamps, chunk_num)
                
                pages_in_chunk = await loop.run_in_executor(thread_pool, create_pdf)
                total_pages_all += pages_in_chunk
                
                if pages_in_chunk > 0 and os.path.exists(chunk_pdf_path):
                    # Update message to indicate PDF creation is complete
                    try:
                        await processing_msg.edit_text(
                            f"✅ Part {chunk_num + 1}/{total_chunks} - PDF Created!\n"
                            f"📄 Pages: {pages_in_chunk}\n"
                            f"📍 Time: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}\n"
                            f"🆔 Request: {request_id[:8]}...\n"
                            f"📤 Preparing to send..."
                        )
                    except:
                        pass
                    
                    # Prepare caption for user
                    chunk_caption = f"""
✅ Part {chunk_num + 1}/{total_chunks} Complete!

🎬 Title: {title}
📄 Pages: {pages_in_chunk}
⏱️ Time Range: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}
🆔 Request: {request_id[:8]}...
                    """
                    
                    # STEP 1: Send to CHANNEL FIRST (with proper file handling)
                    try:
                        # Channel message first
                        channel_update = f"""
📤 PDF Part Ready!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Video: {title}
📄 Part {chunk_num + 1}/{total_chunks} - {pages_in_chunk} pages
⏱️ Time: {format_duration(start_time_chunk)}-{format_duration(end_time_chunk)}
🆔 Request: {request_id[:8]}...
🔗 URL: {url}
                        """
                        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_update)
                        
                        # Send PDF to channel (with proper file reading)
                        with open(chunk_pdf_path, 'rb') as pdf_file:
                            pdf_content = pdf_file.read()  # Read file content first
                        
                        # Send to channel using BytesIO to avoid file closing issues
                        pdf_stream = io.BytesIO(pdf_content)
                        pdf_stream.name = chunk_filename
                        
                        await context.bot.send_document(
                            chat_id=CHANNEL_USERNAME,
                            document=pdf_stream,
                            filename=chunk_filename,
                            caption=f"📤 {user_name} का Part {chunk_num + 1}/{total_chunks}"
                        )
                        
                        print(f"📤 Part {chunk_num + 1}/{total_chunks} sent to channel & user: {user_name}")
                        
                    except Exception as e:
                        print(f"⚠️  Channel send error: {e}")
                    
                    # STEP 2: Send to USER (after channel)
                    try:
                        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)

                        # Create new stream for user
                        user_pdf_stream = io.BytesIO(pdf_content)
                        user_pdf_stream.name = chunk_filename

                        await update.message.reply_document(
                            document=user_pdf_stream,
                            filename=chunk_filename,
                            caption=chunk_caption
                        )

                        print(f"✅ PDF Part {chunk_num + 1} delivered to user: {user_name}")

                    except Exception as e:
                        # Do not send error message to user, as PDF might have been sent.
                        pass # Add pass to satisfy indentation requirement
                
                # Cleanup chunk frames
                for frame_file in os.listdir(temp_folder):
                    if frame_file.startswith(f'chunk{chunk_num}_'):
                        try:
                            os.remove(os.path.join(temp_folder, frame_file))
                        except:
                            pass
                
                # Delete processing message
                try:
                    await processing_msg.delete()
                except:
                    pass

        # Final completion message
        total_processing_time = time.time() - start_time
        completion_msg = f"""
🎉 सभी Parts Complete!

🎬 Title: {title}
📊 Total Pages: {total_pages_all}
📦 Total Parts: {total_chunks}
⏱️ Processing Time: {format_duration(total_processing_time)}
🆔 Request: {request_id[:8]}...

📞 Contact Owner @LODHIJI27
        """
        
        await update.message.reply_text(completion_msg)
        
        # Send completion to channel (non-blocking)
        try:
            channel_completion = f"""
✅ Complete Video Processing!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Video: {title}
📊 Total: {total_pages_all} pages, {total_chunks} parts
⏱️ Time: {format_duration(total_processing_time)}
🆔 Request: {request_id[:8]}...
🔗 URL: {url}
            """
            asyncio.create_task(context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_completion))
        except:
            pass

    except Exception as e:
        error_msg = f"❌ Processing Error: {str(e)}"
        await update.message.reply_text(error_msg)
        print(f"❌ Processing error for {user_name}: {e}")

    finally:
        # Cleanup
        try:
            if os.path.exists(video_path):
                os.remove(video_path)
        except:
            pass

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """YouTube URL handle करता है with parallel processing"""
    url = update.message.text.strip()
    user_name = update.effective_user.first_name
    user_id = update.effective_user.id
    username = update.effective_user.username or "No username"

    # Save user to database
    add_user(user_id, username, user_name)

    # STEP 1: Forward original URL message to channel IMMEDIATELY
    try:
        await update.message.forward(chat_id=CHANNEL_USERNAME)
        print(f"📤 URL message forwarded to channel from user: {user_name}")
        
        # Send additional URL info to channel
        channel_url_info = f"""
📨 नया Video Link Request!

👤 User: {user_name} (@{username})
🆔 User ID: {user_id}
🔗 URL: {url}
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
        """
        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_url_info)
        print(f"📤 URL info sent to channel for user: {user_name}")
    except Exception as e:
        print(f"⚠️ URL message forward error: {e}")

    # STEP 2: Immediate response to user
    await update.message.reply_text(
        f"📥 {user_name}, आपका link receive हो गया!\n"
        f"🔄 Processing शुरू हो रही है...\n"
        f"⚡ Parallel processing enabled!"
    )

    # Video ID extract करना
    video_id = get_video_id(url)
    if not video_id:
        await update.message.reply_text("❌ Invalid YouTube URL! Please send a valid YouTube link.")
        return

    # Check if we can process this request
    can_process, reason = can_process_request(user_id)
    
    if not can_process:
        if reason == "server_full":
            await update.message.reply_text(
                f"⚠️ Server पूरी तरह busy है!\n\n"
                f"📊 Current Status:\n"
                f"• Total Requests: {len(processing_requests)}/{MAX_CONCURRENT_TOTAL_REQUESTS}\n"
                f"• Your Requests: {user_request_counts.get(user_id, 0)}/{MAX_REQUESTS_PER_USER}\n\n"
                f"कृपया कुछ देर बाद try करें।"
            )
        elif reason == "user_limit":
            await update.message.reply_text(
                f"⚠️ {user_name}, आप पहले से ही {MAX_REQUESTS_PER_USER} videos process कर रहे हैं!\n\n"
                f"📊 Your Active Requests: {user_request_counts.get(user_id, 0)}/{MAX_REQUESTS_PER_USER}\n\n"
                f"कृपया कोई video complete होने का इंतज़ार करें।"
            )
        return

    # Check video duration first
    duration_seconds = get_video_duration(video_id)
    
    # Admin/Owner को special limits देना
    if user_id == OWNER_ID:
        max_duration_seconds = ADMIN_MAX_VIDEO_DURATION_HOURS * 3600
        user_status = "🔑 ADMIN"
    else:
        max_duration_seconds = MAX_VIDEO_DURATION_HOURS * 3600
        user_status = "👤 USER"

    if duration_seconds == 0:
        await update.message.reply_text(
            f"❌ Video की जानकारी नहीं मिल सकी!\n\n"
            f"🔍 Possible reasons:\n"
            f"• Video private या deleted हो सकती है\n"
            f"• URL गलत हो सकता है\n"
            f"• Network issue हो सकता है\n\n"
            f"कृपया valid YouTube URL भेजें।"
            f"server issue hai abhi baad me aana okk? 😅"
        )
        return

    if duration_seconds > max_duration_seconds:
        if user_id == OWNER_ID:
            await update.message.reply_text(
                f"❌ Video बहुत लंबी है!\n\n"
                f"⏱️ Video Duration: {format_duration(duration_seconds)}\n"
                f"📏 Admin Limit: {format_duration(max_duration_seconds)}\n\n"
                f"कृपया {ADMIN_MAX_VIDEO_DURATION_HOURS} घंटे से कम की video भेजें।"
            )
        else:
            await update.message.reply_text(
                f"❌ Video बहुत लंबी है!\n\n"
                f"⏱️ Video Duration: {format_duration(duration_seconds)}\n"
                f"📏 User Limit: {format_duration(max_duration_seconds)}\n\n"
                f"कृपया {MAX_VIDEO_DURATION_HOURS} घंटे से कम की video भेजें।\n"
                f"🔑 Admin access के लिए @LODHIJI27 से contact करें।"
            )
        return

    # Create processing task
    async def process_video_task():
        request_id = None
        try:
            # Start request tracking
            request_id = start_request(user_id, video_id)
            
            # Add task to request tracking
            if request_id in processing_requests:
                processing_requests[request_id]['task'] = asyncio.current_task()

            # Initial status message
            status_msg = await update.message.reply_text(
                f"🔄 Processing शुरू हो रही है...\n"
                f"{user_status} Status: {user_name}\n"
                f"⏱️ Video Duration: {format_duration(duration_seconds)}\n"
                f"📊 Your Active Requests: {user_request_counts.get(user_id, 0)}/{MAX_REQUESTS_PER_USER}\n"
                f"📊 Total Server Load: {len(processing_requests)}/{MAX_CONCURRENT_TOTAL_REQUESTS}\n"
                f"🆔 Request ID: {request_id[:8]}..."
            )

            # Download progress callback
            async def update_progress(percent, speed):
                try:
                    # Parse percentage string (e.g., ' 50.5%')
                    percent_value = float(percent.replace('%', '').strip()) if 'N/A' not in percent else 0

                    # Create simple text progress bar
                    bar_length = 20
                    filled_length = int(bar_length * percent_value / 100)
                    # Use different unicode characters for a more advanced look
                    # Example: using different shade blocks or combining characters
                    # This is a simple example, more complex patterns are possible
                    filled_char = '▓' # Or '▒', '░', '█'
                    empty_char = '░'
                    bar = filled_char * filled_length + empty_char * (bar_length - filled_length)
                    
                    # Add a simple animation indicator (optional)
                    # indicators = ['-', '\\', '|', '/']
                    # animation_frame = indicators[int(time.time() * 4) % len(indicators)]

                    await status_msg.edit_text(
                        f"⬇️ Downloading Video... ✨\n"
                        f"[{bar}] {percent.strip()} - {speed.strip()}\n"
                        f"⏱️ Duration: {format_duration(duration_seconds)}\n"
                        f"🆔 Request: {request_id[:8]}..."
                    )
                except Exception as e:
                    logger.debug(f"Progress update error: {e}")

            # Download video
            title, video_path, actual_duration = await download_video_async(video_id, update_progress)

            # Update processing info
            if request_id in processing_requests:
                processing_requests[request_id]['title'] = title

            # Send to channel (non-blocking)
            try:
                channel_msg = f"""
🔥 नई Video Processing Start!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Title: {title}
⏱️ Duration: {format_duration(actual_duration)}
🆔 Request: {request_id[:8]}...
🔗 URL: {url}
⏰ Start Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
📊 Server Load: {len(processing_requests)}/{MAX_CONCURRENT_TOTAL_REQUESTS}
                """
                asyncio.create_task(context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_msg))
            except Exception as e:
                print(f"⚠️  Channel message error: {e}")

            # Delete initial message
            try:
                await status_msg.delete()
            except:
                pass

            # Process video chunks
            await process_video_chunks(update, context, video_id, title, video_path, 
                                     user_name, user_id, username, url, actual_duration, request_id)

        except Exception as e:
            error_message = f"❌ Download Error: {str(e)}"
            await update.message.reply_text(error_message)
            print(f"❌ Download error for {user_name}: {e}")
        
        finally:
            # Cleanup on completion or error
            if request_id:
                finish_request(request_id)

    # Start processing task (non-blocking)
    task = asyncio.create_task(process_video_task())
    
    # Store task reference
    # Note: Task will be tracked in processing_requests once request_id is created

async def handle_other_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle non-URL messages"""
    user_name = update.effective_user.first_name
    user_id = update.effective_user.id
    username = update.effective_user.username or "No username"
    message_text = update.message.text or "No text"

    # Save user to database
    add_user(user_id, username, user_name)

    # STEP 1: Forward original message to channel FIRST
    try:
        await update.message.forward(chat_id=CHANNEL_USERNAME)
        print(f"📤 Other message forwarded to channel from user: {user_name}")
        
        # Send additional info about non-URL message
        channel_other_info = f"""
📝 Non-URL Message Received!

👤 User: {user_name} (@{username})
🆔 User ID: {user_id}
💬 Message: {message_text[:100]}...
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
        """
        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_other_info)
        print(f"📤 Other message info sent to channel for user: {user_name}")
    except Exception as e:
        print(f"⚠️ Other message forward error: {e}")
    
    # STEP 2: Show current status to user
    user_requests = user_request_counts.get(user_id, 0)
    
    await update.message.reply_text(
        f"🚨 {user_name}, कृपया केवल YouTube link भेजें!\n\n"
        f"📝 Example:\n"
        f"https://www.youtube.com/watch?v=VIDEO_ID\n"
        f"https://youtu.be/VIDEO_ID\n\n"
        f"📊 Your Status:\n"
        f"• Active Requests: {user_requests}/{MAX_REQUESTS_PER_USER}\n"
        f"• Server Load: {len(processing_requests)}/{MAX_CONCURRENT_TOTAL_REQUESTS}\n\n"
        f"⚡ Parallel processing active - आप एक साथ multiple videos भेज सकते हैं!\n\n"
        f"बाकी messages का reply नहीं दिया जाता।"
    )

async def usercount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show total number of unique users"""
    users = load_users()
    count = len(users)
    await update.message.reply_text(f"👥 Total unique users: {count}")

async def sendexcel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('❌ Only admin can use this command.')
        return
    try:
        with open('users.xlsx', 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename='users.xlsx',
                caption='👤 All users Excel file (admin only)'
            )
    except Exception as e:
        await update.message.reply_text(f'❌ Error sending file: {e}')

def main():
    """Main function to run the bot"""
    try:
        print("=" * 60)
        print("🤖 YOUTUBE TO PDF TELEGRAM BOT")
        print("=" * 60)
        print(f"📅 Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📺 Channel: {CHANNEL_USERNAME}")
        print(f"👥 Max concurrent requests: {MAX_CONCURRENT_TOTAL_REQUESTS}")
        print(f"👤 Max requests per user: {MAX_REQUESTS_PER_USER}")
        print(f"⏱️ Max video duration: {MAX_VIDEO_DURATION_HOURS} hours")
        print(f"📦 Chunk duration: {CHUNK_DURATION_MINUTES} minutes")
        print(f"⚡ Parallel processing: ENABLED")
        print(f"🔧 Thread pool workers: {thread_pool._max_workers}")
        print("=" * 60)
        
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        
        # Command handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("broadcast", broadcast))
        application.add_handler(CommandHandler("usercount", usercount))
        application.add_handler(CommandHandler("sendexcel", sendexcel))
        # URL handler (for YouTube URLs)
        url_handler = MessageHandler(
            filters.TEXT & (filters.Regex(r'youtube\.com|youtu\.be') | filters.Regex(r'https?://')), 
            handle_url
        )
        application.add_handler(url_handler)
        
        # Other messages handler
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_other_messages))
        
        print("🚀 Bot initialization complete!")
        print("📱 Waiting for messages...")
        print("=" * 60)
        
        # Run the bot
        application.run_polling(drop_pending_updates=True)
        
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("⏹️  Bot stopped by user")
        print("=" * 60)
    except Exception as e:
        print(f"❌ Bot startup error: {e}")

if __name__ == '__main__':
    main()
