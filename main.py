
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

# Your Telegram Bot Token - Get from environment variable with fallback
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '7960013115:AAEocB5fZ6jxLZVIcWwMVd5bJd-oQNqdEfA')

# Channel की settings
CHANNEL_USERNAME = '@alluserpdf'  # आपका channel username

# SSIM के लिए सेटिंग्स
SSIM_THRESHOLD = 1  # समानता का थ्रेशोल्ड
SSIM_RESIZE_DIM = (128, 72) # SSIM तुलना के लिए फ्रेम का आकार
FRAME_SKIP_FOR_SSIM_CHECK = 500 # हर 3rd फ्रेम पर SSIM जांच

# PDF के लिए सेटिंग्स
PDF_FRAME_WIDTH_TARGET = 1280 # PDF में फ्रेम की चौड़ाई
WATERMARK_TEXT = "Created by @youpdf_bot"
MAX_PDF_PAGES = 5000 # PDF में अधिकतम पेज

# Multi-user processing के लिए settings
MAX_CONCURRENT_USERS = 10
CHUNK_DURATION_MINUTES = 30  # 30 मिनट के chunks
MAX_VIDEO_DURATION_HOURS = 1.5 # अधिकतम 1.5 घंटे

# Semaphore for limiting concurrent processing
processing_semaphore = Semaphore(MAX_CONCURRENT_USERS)
user_queue = []
processing_users = {}  # Changed to dict to store user processing info

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
            duration = info_dict.get('duration', 0)  # seconds में
            return duration
        except:
            return 0

def download_video(video_id, progress_callback=None):
    """YouTube video download करता है with better control"""
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    output_file = f"video_{video_id}.mp4"
    
    # Clean console output
    def progress_hook(d):
        if progress_callback and d['status'] == 'downloading':
            try:
                percent = d.get('_percent_str', 'N/A').strip()
                speed = d.get('_speed_str', 'N/A').strip()
                if progress_callback:
                    progress_callback(percent, speed)
            except:
                pass
    
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
            frame = cv2.resize(frame, (640, 360), interpolation=cv2.INTER_CUBIC)
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
    pdf.set_auto_page_break(0)

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user_name = update.effective_user.first_name
    user_id = update.effective_user.id
    username = update.effective_user.username or "No username"

    welcome_message = f"""
👋 नमस्ते {user_name}!

🎬 YouTube to PDF Bot में आपका स्वागत है!

📋 कैसे काम करता है:
1. YouTube video का link भेजें 
2. Bot video को 30-30 मिनट के भागों में बांटेगा
3. हर भाग की PDF बनकर तुरंत भेजी जाएगी
4. एक साथ 10 users की videos process हो सकती हैं


🚨 Bot को लिंक के अलावा कोई और मैसेज न करें 
यह मैसेज Owner के पास नहीं जाता है
📞 Contact Owner - @LODHIJI27

बस YouTube link भेजिए! 🚀

⚠️ नोट: केवल 1.5 घंटे तक की videos ही process होंगी
    """

    await update.message.reply_text(welcome_message)

    # Forward original message to channel
    try:
        await update.message.forward(chat_id=CHANNEL_USERNAME)
        
        # Additional info message for channel
        channel_message = f"""
🆕 नया User Bot को Start किया!

👤 Name: {user_name}
🆔 User ID: {user_id}
📝 Username: @{username}
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
        """
        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_message)
    except Exception as e:
        print(f"Channel message send error: {e}")

async def process_video_chunks(update, context, video_id, title, video_path, user_name, user_id, username, url, duration_seconds):
    """Video को chunks में process करता है और हर chunk की PDF instantly भेजता है"""
    messages_to_delete = []  # Track messages to delete
    start_time = time.time()
    
    try:
        chunk_duration_seconds = CHUNK_DURATION_MINUTES * 60
        total_chunks = int(np.ceil(duration_seconds / chunk_duration_seconds))
        
        analysis_msg = await update.message.reply_text(
            f"📊 Video Analysis:\n"
            f"🎬 Title: {title}\n"
            f"⏱️ कुल समय: {format_duration(duration_seconds)}\n"
            f"📦 कुल भाग: {total_chunks}\n\n"
            f"🔄 Processing शुरू हो रही है..."
        )
        
        # Forward analysis to channel
        try:
            await analysis_msg.forward(chat_id=CHANNEL_USERNAME)
        except:
            pass

        total_pages_all = 0

        with tempfile.TemporaryDirectory() as temp_folder:
            for chunk_num in range(total_chunks):
                start_time_chunk = chunk_num * chunk_duration_seconds
                end_time_chunk = min((chunk_num + 1) * chunk_duration_seconds, duration_seconds)
                
                chunk_start_min = int(start_time_chunk // 60)
                chunk_end_min = int(end_time_chunk // 60)
                
                # Better progress message
                processing_msg = await update.message.reply_text(
                    f"🔄 Processing Part {chunk_num + 1}/{total_chunks}\n"
                    f"📍 Video portion: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}\n"
                    f"⚙️ Extracting frames..."
                )
                messages_to_delete.append(processing_msg)
                
                # Forward processing update to channel
                try:
                    await processing_msg.forward(chat_id=CHANNEL_USERNAME)
                except:
                    pass
                
                # Frames extract करना
                timestamps = extract_unique_frames_for_chunk(
                    video_path, temp_folder, start_time_chunk, end_time_chunk, chunk_num, 
                    n=FRAME_SKIP_FOR_SSIM_CHECK, ssim_threshold=SSIM_THRESHOLD
                )
                
                if not timestamps:
                    await update.message.reply_text(f"⚠️ Part {chunk_num + 1}: कोई unique frames नहीं मिले")
                    continue
                
                # PDF बनाना
                try:
                    await processing_msg.edit_text(
                        f"🔄 Processing Part {chunk_num + 1}/{total_chunks}\n"
                        f"📍 Video portion: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}\n"
                        f"📄 Creating PDF... ({len(timestamps)} frames)"
                    )
                except:
                    pass
                
                # Chunk का filename
                safe_title = sanitize_filename(title)[:50]
                chunk_filename = f"{safe_title}_Part{chunk_num + 1}_of_{total_chunks}.pdf"
                chunk_pdf_path = os.path.join(temp_folder, chunk_filename)
                
                # PDF convert करना
                pages_in_chunk = convert_frames_to_pdf_chunk(temp_folder, chunk_pdf_path, timestamps, chunk_num)
                total_pages_all += pages_in_chunk
                
                if pages_in_chunk > 0 and os.path.exists(chunk_pdf_path):
                    # PDF भेजना
                    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
                    
                    chunk_caption = f"""
✅ Part {chunk_num + 1}/{total_chunks} Complete!

🎬 Title: {title}
📄 Pages: {pages_in_chunk}
⏱️ Time Range: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}
📦 Frames: {len(timestamps)}

📞 Bot by @LODHIJI27
                    """
                    
                    with open(chunk_pdf_path, 'rb') as pdf_file:
                        await update.message.reply_document(
                            document=pdf_file,
                            filename=chunk_filename,
                            caption=chunk_caption
                        )
                    
                    # Send to channel
                    try:
                        channel_update = f"""
📤 PDF Part Sent!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Video: {title}
📄 Part {chunk_num + 1}/{total_chunks} - {pages_in_chunk} pages
⏱️ Time: {format_duration(start_time_chunk)}-{format_duration(end_time_chunk)}
🔗 URL: {url}
                        """
                        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_update)
                    except:
                        pass
                
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

👨‍💻 Bot by @LODHIJI27
        """
        
        await update.message.reply_text(completion_msg)
        
        # Send completion to channel
        try:
            channel_completion = f"""
✅ Complete Video Processing!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Video: {title}
📊 Total: {total_pages_all} pages, {total_chunks} parts
⏱️ Time: {format_duration(total_processing_time)}
🔗 URL: {url}
            """
            await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_completion)
        except:
            pass

    except Exception as e:
        error_msg = f"❌ Error during processing: {str(e)}"
        await update.message.reply_text(error_msg)
        print(f"Chunk processing error: {e}")

    finally:
        # Clean up video file
        try:
            if os.path.exists(video_path):
                os.remove(video_path)
        except:
            pass

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """YouTube URL handle करता है"""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    username = update.effective_user.username or "No username"
    url = update.message.text.strip()
    
    # Check if user is already processing
    if user_id in processing_users:
        await update.message.reply_text(
            f"⚠️ {user_name}, आपकी एक video पहले से ही process हो रही है!\n"
            f"कृपया पहली video का processing complete होने का इंतजार करें।"
        )
        return
    
    # YouTube URL validation
    if not re.search(r'(youtube\.com|youtu\.be)', url):
        await update.message.reply_text(
            "❌ कृपया valid YouTube link भेजें!\n\n"
            "Example:\n"
            "https://www.youtube.com/watch?v=VIDEO_ID\n"
            "https://youtu.be/VIDEO_ID"
        )
        return
    
    # Extract video ID
    video_id = get_video_id(url)
    if not video_id:
        await update.message.reply_text("❌ YouTube video ID extract नहीं हो सका!")
        return
    
    # Check video duration first
    duration_seconds = get_video_duration(video_id)
    max_duration_seconds = MAX_VIDEO_DURATION_HOURS * 3600
    
    if duration_seconds == 0:
        await update.message.reply_text("❌ Video की information प्राप्त नहीं हो सकी!")
        return
    
    if duration_seconds > max_duration_seconds:
        await update.message.reply_text(
            f"❌ Video बहुत लंबी है!\n\n"
            f"📏 Video Duration: {format_duration(duration_seconds)}\n"
            f"🚫 Maximum Allowed: {format_duration(max_duration_seconds)}\n\n"
            f"कृपया {MAX_VIDEO_DURATION_HOURS} घंटे से कम की video भेजें।"
        )
        return
    
    # Check if we can acquire semaphore (non-blocking)
    if not processing_semaphore.acquire(blocking=False):
        queue_position = len(user_queue) + 1
        user_queue.append(user_id)
        
        await update.message.reply_text(
            f"⏳ Queue में आपका स्थान: {queue_position}\n\n"
            f"🔄 अभी {MAX_CONCURRENT_USERS} users की videos process हो रही हैं।\n"
            f"⏰ आपकी बारी आने पर processing शुरू होगी।\n\n"
            f"कृपया थोड़ा इंतजार करें..."
        )
        
        # Wait for turn
        while user_id in user_queue and not processing_semaphore.acquire(blocking=False):
            await asyncio.sleep(5)
        
        if user_id in user_queue:
            user_queue.remove(user_id)
    
    # Add user to processing list
    processing_users[user_id] = {
        'video_id': video_id,
        'start_time': time.time(),
        'url': url
    }
    
    try:
        # Forward original URL message to channel
        try:
            await update.message.forward(chat_id=CHANNEL_USERNAME)
            
            # Additional info message for channel
            channel_message = f"""
🔗 नया YouTube Link Received!

👤 Name: {user_name}
🆔 User ID: {user_id}
📝 Username: @{username}
🎬 Video ID: {video_id}
⏱️ Duration: {format_duration(duration_seconds)}
🔗 URL: {url}
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
            """
            await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_message)
        except Exception as e:
            print(f"Channel message send error: {e}")
        
        # Start processing
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
        
        # Initial response
        progress_msg = await update.message.reply_text(
            f"🎬 Video Processing शुरू...\n\n"
            f"📋 Video ID: {video_id}\n"
            f"⏱️ Duration: {format_duration(duration_seconds)}\n"
            f"📥 Downloading video..."
        )
        
        # Download progress callback
        async def update_progress(percent, speed):
            try:
                await progress_msg.edit_text(
                    f"🎬 Video Processing...\n\n"
                    f"📋 Video ID: {video_id}\n"
                    f"⏱️ Duration: {format_duration(duration_seconds)}\n"
                    f"📥 Download Progress: {percent}\n"
                    f"🚀 Speed: {speed}"
                )
            except:
                pass
        
        # Download video in thread to avoid blocking
        loop = asyncio.get_event_loop()
        
        def download_wrapper():
            return download_video(video_id, lambda p, s: asyncio.run_coroutine_threadsafe(update_progress(p, s), loop))
        
        with ThreadPoolExecutor() as executor:
            future = loop.run_in_executor(executor, download_wrapper)
            title, video_path, duration = await future
        
        # Update message after download
        try:
            await progress_msg.edit_text(
                f"✅ Video Downloaded!\n\n"
                f"🎬 Title: {title}\n"
                f"⏱️ Duration: {format_duration(duration)}\n"
                f"📂 File: {video_path}\n\n"
                f"🔄 Starting frame extraction..."
            )
        except:
            pass
        
        # Process video chunks
        await process_video_chunks(
            update, context, video_id, title, video_path, 
            user_name, user_id, username, url, duration
        )
        
    except Exception as e:
        error_message = f"❌ Error: {str(e)}"
        await update.message.reply_text(error_message)
        print(f"Processing error for user {user_id}: {e}")
        
        # Send error to channel
        try:
            channel_error = f"""
❌ Processing Error!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Video ID: {video_id}
🔗 URL: {url}
❌ Error: {str(e)}
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
            """
            await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_error)
        except:
            pass
    
    finally:
        # Clean up and release resources
        if user_id in processing_users:
            del processing_users[user_id]
        
        processing_semaphore.release()
        
        # Process next user in queue if any
        if user_queue:
            next_user = user_queue.pop(0)
            # The next user's handler will automatically acquire the semaphore

async def handle_other_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle non-URL messages"""
    user_name = update.effective_user.first_name
    
    await update.message.reply_text(
        f"👋 {user_name}, मैं केवल YouTube links process करता हूं!\n\n"
        f"🎬 कृपया YouTube video का link भेजें।\n\n"
        f"Example:\n"
        f"https://www.youtube.com/watch?v=VIDEO_ID\n"
        f"https://youtu.be/VIDEO_ID\n\n"
        f"📞 Help के लिए: @LODHIJI27"
    )
    
    # Forward to channel for monitoring
    try:
        await update.message.forward(chat_id=CHANNEL_USERNAME)
        
        channel_message = f"""
💬 Other Message Received

👤 User: {user_name}
🆔 ID: {update.effective_user.id}
📝 Username: @{update.effective_user.username or 'No username'}
💬 Message: {update.message.text[:100]}...
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
        """
        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_message)
    except Exception as e:
        print(f"Channel forward error: {e}")

def main():
    """Main function to run the bot"""
    print("🚀 YouTube to PDF Bot Starting...")
    print(f"📊 Max concurrent users: {MAX_CONCURRENT_USERS}")
    print(f"⏱️ Chunk duration: {CHUNK_DURATION_MINUTES} minutes")
    print(f"📺 Max video duration: {MAX_VIDEO_DURATION_HOURS} hours")
    
    # Create bot application
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    
    # URL handler (YouTube links)
    url_filter = filters.Regex(r'(youtube\.com|youtu\.be)')
    application.add_handler(MessageHandler(url_filter, handle_url))
    
    # Other messages handler
    application.add_handler(MessageHandler(filters.TEXT & ~url_filter, handle_other_messages))
    
    print("✅ Bot handlers registered")
    print("🔄 Starting polling...")
    
    # Start the bot
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

if __name__ == '__main__':
    main()
