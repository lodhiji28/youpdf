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
FRAME_SKIP_FOR_SSIM_CHECK = 250 # हर 3rd फ्रेम पर SSIM जांच

# PDF के लिए सेटिंग्स
PDF_FRAME_WIDTH_TARGET = 1280 # PDF में फ्रेम की चौड़ाई
WATERMARK_TEXT = "Created by @youpdf_bot"
MAX_PDF_PAGES = 5000 # PDF में अधिकतम पेज

# Multi-user processing के लिए settings
MAX_CONCURRENT_USERS = 10
CHUNK_DURATION_MINUTES = 30  # 30 मिनट के chunks
MAX_VIDEO_DURATION_HOURS = 1.5 # अधिकतम 1.5 घंटे
ADMIN_MAX_VIDEO_DURATION_HOURS = 50 # Admin के लिए अधिकतम 50 घंटे

# Admin/Owner की ID
OWNER_ID = 2141959380

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
    
    # Try multiple configurations with different client types
    configs = [
        {
            'quiet': True,
            'no_warnings': True,
            'format': 'worst',
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                }
            }
        },
        {
            'quiet': True,
            'no_warnings': True,
            'format': 'worst',
            'extractor_args': {
                'youtube': {
                    'player_client': ['ios'],
                }
            }
        },
        {
            'quiet': True,
            'no_warnings': True,
            'cookiefile': 'cookies.txt',
            'format': 'worst',
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                }
            }
        }
    ]
    
    for ydl_opts in configs:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info_dict = ydl.extract_info(video_url, download=False)
                if info_dict:
                    duration = info_dict.get('duration', 0)
                    if duration > 0:
                        return duration
            except Exception as e:
                print(f"Duration check attempt failed for {video_id}: {e}")
                continue
    
    print(f"All duration check methods failed for {video_id}")
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
    
    # Try different download configurations
    configs = [
        {
            'format': 'best[height<=720]/best',
            'outtmpl': output_file,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'progress_hooks': [progress_hook],
            'retries': 3,
            'fragment_retries': 3,
            'extractaudio': False,
            'keepvideo': True,
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                }
            }
        },
        {
            'format': 'best[height<=720]/best',
            'outtmpl': output_file,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'progress_hooks': [progress_hook],
            'retries': 3,
            'fragment_retries': 3,
            'extractaudio': False,
            'keepvideo': True,
            'extractor_args': {
                'youtube': {
                    'player_client': ['ios'],
                }
            }
        },
        {
            'format': 'best[height<=720]/best',
            'outtmpl': output_file,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'progress_hooks': [progress_hook],
            'retries': 2,
            'fragment_retries': 2,
            'extractaudio': False,
            'keepvideo': True,
            'cookiefile': 'cookies.txt',
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                }
            }
        }
    ]
    
    for i, ydl_opts in enumerate(configs):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(video_url, download=True)
                title = info_dict.get('title', 'Unknown Title')
                duration = info_dict.get('duration', 0)
                
                if not os.path.exists(output_file):
                    raise Exception("Video file download failed")
                    
                return title, output_file, duration
                
        except Exception as e:
            print(f"Download attempt {i+1} failed for {video_id}: {e}")
            if os.path.exists(output_file):
                try:
                    os.remove(output_file)
                except:
                    pass
            
            if i < len(configs) - 1:  # Not the last attempt
                continue
            else:  # Last attempt failed
                raise Exception(f"All download methods failed: {str(e)}")

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
                    # First send to channel, then to user
                    chunk_caption = f"""
✅ Part {chunk_num + 1}/{total_chunks} Complete!

🎬 Title: {title}
📄 Pages: {pages_in_chunk}
⏱️ Time Range: {format_duration(start_time_chunk)} - {format_duration(end_time_chunk)}

                    """
                    
                    # Send to channel FIRST
                    try:
                        channel_update = f"""
📤 PDF Part Ready!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Video: {title}
📄 Part {chunk_num + 1}/{total_chunks} - {pages_in_chunk} pages
⏱️ Time: {format_duration(start_time_chunk)}-{format_duration(end_time_chunk)}
🔗 URL: {url}
                        """
                        await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_update)
                        
                        # Send PDF to channel
                        with open(chunk_pdf_path, 'rb') as pdf_file:
                            await context.bot.send_document(
                                chat_id=CHANNEL_USERNAME,
                                document=pdf_file,
                                filename=chunk_filename,
                                caption=f"📤 {user_name} का Part {chunk_num + 1}/{total_chunks}"
                            )
                    except Exception as e:
                        print(f"Channel send error: {e}")
                    
                    # Now send to user
                    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_DOCUMENT)
                    
                    with open(chunk_pdf_path, 'rb') as pdf_file:
                        await update.message.reply_document(
                            document=pdf_file,
                            filename=chunk_filename,
                            caption=chunk_caption
                        )
                
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

📞 Contact Owner @LODHIJI27
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
        error_msg = f"❌ Processing Error: {str(e)}"
        await update.message.reply_text(error_msg)
        print(f"Processing error for {user_name}: {e}")

    finally:
        # Cleanup
        try:
            if os.path.exists(video_path):
                os.remove(video_path)
        except:
            pass
        
        # Remove from processing
        if user_id in processing_users:
            del processing_users[user_id]
        
        # Release semaphore
        processing_semaphore.release()
        
        # Process next in queue
        await process_next_in_queue(context)

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """YouTube URL handle करता है"""
    url = update.message.text.strip()
    user_name = update.effective_user.first_name
    user_id = update.effective_user.id
    username = update.effective_user.username or "No username"

    # Video ID extract करना
    video_id = get_video_id(url)
    if not video_id:
        await update.message.reply_text("❌ Invalid YouTube URL! Please send a valid YouTube link.")
        return

    # Check if user is already being processed
    if user_id in processing_users:
        await update.message.reply_text(
            f"⚠️ {user_name}, आपकी एक video पहले से process हो रही है!\n"
            f"कृपया current video complete होने का इंतज़ार करें।"
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

    # Try to acquire semaphore (non-blocking)
    if not processing_semaphore.acquire(blocking=False):
        # Add to queue
        queue_position = len(user_queue) + 1
        user_queue.append({
            'user_id': user_id,
            'user_name': user_name,
            'username': username,
            'url': url,
            'video_id': video_id,
            'update': update,
            'context': context,
            'duration_seconds': duration_seconds
        })
        
        await update.message.reply_text(
            f"⏳ आपकी video queue में add हो गई है!\n\n"
            f"📊 Queue Position: {queue_position}\n"
            f"👥 Currently Processing: {MAX_CONCURRENT_USERS} users\n"
            f"⏱️ Video Duration: {format_duration(duration_seconds)}\n\n"
            f"कृपया अपनी बारी का इंतज़ार करें।"
        )
        return

    # Process immediately
    processing_users[user_id] = {
        'start_time': time.time(),
        'video_title': 'Processing...',
        'user_name': user_name
    }

    try:
        # Initial message
        initial_msg = await update.message.reply_text(
            f"🔄 Processing शुरू हो रही है...\n"
            f"{user_status} Status: {user_name}\n"
            f"⏱️ Video Duration: {format_duration(duration_seconds)}\n"
            f"📊 आप {len(processing_users)}/{MAX_CONCURRENT_USERS} processing slots में से एक का उपयोग कर रहे हैं"
        )

        # Download progress callback
        async def update_progress(percent, speed):
            try:
                await initial_msg.edit_text(
                    f"⬇️ Downloading Video...\n"
                    f"📊 Progress: {percent}\n"
                    f"🚀 Speed: {speed}\n"
                    f"⏱️ Duration: {format_duration(duration_seconds)}"
                )
            except:
                pass

        # Download video in thread
        def download_wrapper():
            return download_video(video_id, lambda percent, speed: asyncio.create_task(update_progress(percent, speed)))

        # Execute download in thread pool
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            title, video_path, actual_duration = await loop.run_in_executor(executor, download_wrapper)

        # Update processing info
        processing_users[user_id]['video_title'] = title

        # Send to channel
        try:
            channel_msg = f"""
🔥 नई Video Processing Start!

👤 User: {user_name} (@{username})
🆔 ID: {user_id}
🎬 Title: {title}
⏱️ Duration: {format_duration(actual_duration)}
🔗 URL: {url}
⏰ Start Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
            """
            await context.bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_msg)
        except Exception as e:
            print(f"Channel message error: {e}")

        # Delete initial message
        try:
            await initial_msg.delete()
        except:
            pass

        # Process video chunks
        await process_video_chunks(update, context, video_id, title, video_path, 
                                 user_name, user_id, username, url, actual_duration)

    except Exception as e:
        error_message = f"❌ Download Error: {str(e)}"
        await update.message.reply_text(error_message)
        print(f"Download error for {user_name}: {e}")
        
        # Cleanup on error
        if user_id in processing_users:
            del processing_users[user_id]
        processing_semaphore.release()
        await process_next_in_queue(context)

async def handle_other_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle non-URL messages"""
    user_name = update.effective_user.first_name
    await update.message.reply_text(
        f"🚨 {user_name}, कृपया केवल YouTube link भेजें!\n\n"
        f"📝 Example:\n"
        f"https://www.youtube.com/watch?v=VIDEO_ID\n"
        f"https://youtu.be/VIDEO_ID\n\n"
        f"बाकी messages का reply नहीं दिया जाता।"
    )

async def process_next_in_queue(context):
    """Queue से अगले user को process करता है"""
    if user_queue and len(processing_users) < MAX_CONCURRENT_USERS:
        next_user = user_queue.pop(0)
        
        # Update queue positions for remaining users
        for i, user in enumerate(user_queue):
            try:
                await user['update'].message.reply_text(
                    f"⏳ Queue Update!\n"
                    f"📊 New Position: {i + 1}\n"
                    f"👥 Currently Processing: {len(processing_users) + 1}/{MAX_CONCURRENT_USERS}"
                )
            except:
                pass
        
        # Process the next user
        await handle_url(next_user['update'], next_user['context'])

def main():
    """Main function to run the bot"""
    try:
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        
        # Command handlers
        application.add_handler(CommandHandler("start", start))
        
        # URL handler (for YouTube URLs)
        url_handler = MessageHandler(
            filters.TEXT & (filters.Regex(r'youtube\.com|youtu\.be') | filters.Regex(r'https?://')), 
            handle_url
        )
        application.add_handler(url_handler)
        
        # Other messages handler
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_other_messages))
        
        print("🤖 Bot is starting...")
        print(f"👥 Max concurrent users: {MAX_CONCURRENT_USERS}")
        print(f"⏱️ Max video duration: {MAX_VIDEO_DURATION_HOURS} hours")
        print(f"📦 Chunk duration: {CHUNK_DURATION_MINUTES} minutes")
        
        # Run the bot
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        print(f"❌ Bot startup error: {e}")

if __name__ == '__main__':
    main()