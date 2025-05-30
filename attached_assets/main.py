
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

# Your Telegram Bot Token
TELEGRAM_TOKEN = '7960013115:AAEocB5fZ6jxLZVIcWwMVd5bJd-oQNqdEfA'

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
                    video_path, temp_folder, start_time_chunk, end_time_chunk, chunk_num
                )
                
                if not timestamps:
                    await processing_msg.edit_text(
                        f"⚠️ Part {chunk_num + 1} में कोई unique frames नहीं मिले"
                    )
                    continue
                
                # Update progress
                await processing_msg.edit_text(
                    f"🔄 Processing Part {chunk_num + 1}/{total_chunks}\n"
                    f"📍 Video portion: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}\n"
                    f"📄 Creating PDF with {len(timestamps)} pages..."
                )
                
                # PDF बनाना
                pdf_file_name = f"{sanitize_filename(title)}_Part_{chunk_num + 1}.pdf"
                total_pages = convert_frames_to_pdf_chunk(temp_folder, pdf_file_name, timestamps, chunk_num)
                
                if total_pages > 0:
                    total_pages_all += total_pages
                    
                    # Update progress
                    await processing_msg.edit_text(
                        f"📤 Sending Part {chunk_num + 1}/{total_chunks}\n"
                        f"📍 Video portion: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}\n"
                        f"📄 Pages: {total_pages}"
                    )
                    
                    # Send to channel first
                    try:
                        with open(pdf_file_name, 'rb') as pdf_file:
                            channel_caption = f"""
📄 PDF Part {chunk_num + 1}/{total_chunks} Ready!

👤 User: {user_name} (@{username})
🆔 User ID: {user_id}
🎬 Title: {title}
📍 Time: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}
📄 Pages: {total_pages}
⏰ Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}
                            """
                            await context.bot.send_document(
                                chat_id=CHANNEL_USERNAME,
                                document=pdf_file,
                                caption=channel_caption,
                                read_timeout=600,
                                write_timeout=600
                            )
                    except Exception as e:
                        print(f"Channel PDF send error: {e}")
                    
                    # Send to user with multiple retry attempts
                    max_retries = 5
                    retry_count = 0
                    sent_successfully = False
                    
                    while retry_count < max_retries and not sent_successfully:
                        try:
                            with open(pdf_file_name, 'rb') as pdf_file:
                                user_caption = f"""
✅ Part {chunk_num + 1}/{total_chunks} Ready!
🎬 {title}
📍 {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}
📄 Pages: {total_pages}
                                """
                                await update.message.reply_document(
                                    pdf_file,
                                    caption=user_caption,
                                    read_timeout=600,
                                    write_timeout=600
                                )
                                sent_successfully = True
                                
                        except Exception as e:
                            retry_count += 1
                            if "Request Entity Too Large" in str(e):
                                print(f"File too large, retrying in 5 seconds... (Attempt {retry_count}/{max_retries})")
                                await asyncio.sleep(5)
                            else:
                                print(f"Send error (Attempt {retry_count}/{max_retries}): {e}")
                                await asyncio.sleep(2)
                    
                    if sent_successfully:
                        # Delete the processing message after successful send
                        try:
                            await processing_msg.delete()
                            messages_to_delete.remove(processing_msg)
                        except:
                            pass
                    else:
                        await processing_msg.edit_text(f"❌ Part {chunk_num + 1} send करने में कई बार कोशिश के बाद भी error आया")
                    
                    # File cleanup
                    try:
                        os.remove(pdf_file_name)
                    except:
                        pass
                else:
                    await processing_msg.edit_text(f"⚠️ Part {chunk_num + 1} में कोई valid frames नहीं मिले")
        
        # Calculate total processing time
        total_time = time.time() - start_time
        
        # Final completion message
        completion_msg = await update.message.reply_text(
            f"""
🎉 Complete! PDF successfully भेजी गई!

Bot को लिंक के अलावा कोई और मैसेज न करें 
यह मैसेज Owner के पास नहीं जाता है
Contact Owner - @LODHIJI27

📄 Total Pages: {total_pages_all}
⏱️ समय: {format_duration(total_time)}
            """
        )
        
        # Forward completion to channel
        try:
            await completion_msg.forward(chat_id=CHANNEL_USERNAME)
        except:
            pass
        
        # Clean up any remaining processing messages
        for msg in messages_to_delete:
            try:
                await msg.delete()
            except:
                pass
        
    except Exception as e:
        error_msg = f"❌ Processing Error: {str(e)}"
        await update.message.reply_text(error_msg)
        
        # Forward error to channel
        try:
            await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=f"Error for user {user_name}: {error_msg}")
        except:
            pass
    finally:
        # User को processing list से हटाना
        if user_id in processing_users:
            del processing_users[user_id]
        
        # Video file cleanup
        try:
            if 'video_path' in locals() and os.path.exists(video_path):
                os.remove(video_path)
        except:
            pass
        
        # Clean up any leftover video files
        try:
            import glob
            for file_pattern in ['video_*.mp4', 'video_*.mp4.part', 'video_*.mp4.ytdl']:
                for file in glob.glob(file_pattern):
                    try:
                        os.remove(file)
                    except:
                        pass
        except:
            pass

async def download_and_convert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main function जो YouTube URL handle करता है"""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    username = update.effective_user.username or "No username"
    url = update.message.text

    # Forward user message to channel
    try:
        await update.message.forward(chat_id=CHANNEL_USERNAME)
        
        user_message_info = f"""
📨 New Video Request:

👤 Name: {user_name}
🆔 User ID: {user_id}
📝 Username: @{username}
💬 Link: {url}
⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
        """
        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=user_message_info)
    except Exception as e:
        print(f"Channel message send error: {e}")

    # Check if user is already processing
    if user_id in processing_users:
        await update.message.reply_text(
            f"⚠️ {user_name} जी, आपकी पहले से ही एक video process हो रही है!\n"
            f"कृपया उसके complete होने का wait करें। 🙏"
        )
        return

    # Check processing capacity and add to queue if needed
    if len(processing_users) >= MAX_CONCURRENT_USERS:
        queue_position = len(user_queue) + 1
        user_queue.append({
            'user_id': user_id,
            'update': update,
            'context': context,
            'url': url,
            'user_name': user_name,
            'username': username
        })
        
        queue_msg = await update.message.reply_text(
            f"⏳ {user_name} जी, आपकी video processing line में लगी है!\n\n"
            f"📍 आपका नंबर: {queue_position}\n"
            f"🔄 Currently {len(processing_users)} users की videos process हो रही हैं\n\n"
            f"कृपया थोड़ा wait करें। जल्दी ही आपकी बारी आएगी! 🙏"
        )
        
        # Forward queue message to channel
        try:
            await queue_msg.forward(chat_id=CHANNEL_USERNAME)
        except:
            pass
        return

    video_id = get_video_id(url)
    if not video_id:
        error_msg = await update.message.reply_text("❌ Invalid YouTube URL! कृपया valid YouTube link भेजें।")
        try:
            await error_msg.forward(chat_id=CHANNEL_USERNAME)
        except:
            pass
        return

    # User को processing में add करना
    processing_users[user_id] = {
        'user_name': user_name,
        'username': username,
        'start_time': time.time()
    }
    
    download_msg = None
    
    try:
        # Storage check करना पहले
        import shutil
        free_space_gb = shutil.disk_usage('.').free / (1024**3)
        if free_space_gb < 2:  # 2GB से कम space है तो warning
            await update.message.reply_text(
                f"⚠️ Storage कम है ({free_space_gb:.1f}GB free)!\n"
                f"कुछ समय बाद try करें।"
            )
            return
        
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
        
        # Video info check करना पहले
        info_msg = await update.message.reply_text("🔍 Video information निकाली जा रही है...")
        
        try:
            duration_seconds = get_video_duration(video_id)
            if duration_seconds == 0:
                await info_msg.edit_text("❌ Video information नहीं मिली। कृपया valid URL check करें।")
                return
                
            if duration_seconds > MAX_VIDEO_DURATION_HOURS * 3600:
                await info_msg.edit_text(
                    f"❌ Video बहुत लंबी है ({format_duration(duration_seconds)})!\n"
                    f"कृपया {MAX_VIDEO_DURATION_HOURS} घंटे से कम की video भेजें।"
                )
                return
                
            await info_msg.edit_text(
                f"✅ Video मिली! ({format_duration(duration_seconds)})\n"
                f"📥 Download शुरू हो रही है..."
            )
            
        except Exception as e:
            await info_msg.edit_text(f"❌ Video info error: {str(e)}")
            return
        
        # Progress tracking के लिए
        download_msg = info_msg
        last_update_time = time.time()
        
        def progress_callback(percent, speed):
            nonlocal last_update_time
            current_time = time.time()
            # हर 2 सेकंड में update करना
            if current_time - last_update_time > 2:
                try:
                    asyncio.create_task(
                        download_msg.edit_text(
                            f"📥 Download Progress: {percent}\n"
                            f"⚡ Speed: {speed}\n"
                            f"🔄 Processing will start after download..."
                        )
                    )
                    last_update_time = current_time
                except:
                    pass
        
        # Video download करना
        try:
            title, video_path, actual_duration = download_video(video_id, progress_callback)
            await download_msg.edit_text(f"✅ '{title}' download complete!\n🔄 Processing शुरू हो रही है...")
            
            # File size check
            if os.path.exists(video_path):
                file_size = os.path.getsize(video_path) / (1024 * 1024)  # MB में
                if file_size < 1:  # 1MB से कम
                    await download_msg.edit_text("❌ Download incomplete या corrupt file!")
                    return
                    
        except Exception as download_error:
            await download_msg.edit_text(f"❌ Download failed: {str(download_error)}")
            return
        
        # Delete download progress message
        try:
            await download_msg.delete()
        except:
            pass
        
        # Video को chunks में process करना
        await process_video_chunks(update, context, video_id, title, video_path, user_name, user_id, username, url, duration_seconds)
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        await update.message.reply_text(error_msg)
        
        # Forward error to channel
        try:
            await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=f"Error for user {user_name}: {error_msg}")
        except:
            pass
    finally:
        # User को processing list से हटाना
        if user_id in processing_users:
            del processing_users[user_id]
    
    # Queue से अगला user process करना
    await process_next_in_queue(context)

async def process_next_in_queue(context):
    """Queue से अगले user को process करता है"""
    if user_queue and len(processing_users) < MAX_CONCURRENT_USERS:
        next_user = user_queue.pop(0)
        
        # Notify user their turn has come
        try:
            turn_msg = await next_user['update'].message.reply_text(
                f"🎉 {next_user['user_name']} जी, आपकी बारी आ गई है!\n"
                f"🔄 आपकी video processing शुरू हो रही है..."
            )
            
            # Forward turn message to channel
            try:
                await turn_msg.forward(chat_id=CHANNEL_USERNAME)
            except:
                pass
            
            # Process their video
            await download_and_convert(next_user['update'], next_user['context'])
        except Exception as e:
            print(f"Error processing queued user: {e}")

def main():
    """Main function"""
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, download_and_convert))
    
    print("🤖 Bot is running...")
    print(f"📊 Max concurrent users: {MAX_CONCURRENT_USERS}")
    print(f"📦 Chunk duration: {CHUNK_DURATION_MINUTES} minutes")
    print(f"⏰ Max video duration: {MAX_VIDEO_DURATION_HOURS} hours")
    
    application.run_polling()

if __name__ == "__main__":
    main()
