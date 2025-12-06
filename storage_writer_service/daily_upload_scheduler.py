"""Schedules daily uploads to S3"""
import logging
import threading
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any

logger = logging.getLogger(__name__)


class DailyUploadScheduler:
    """Schedules daily uploads of buffered data to S3"""
    
    def __init__(self, daily_buffer, local_file_writer, s3_uploader, upload_time: str = "00:00",
                 file_generation_interval_minutes: int = 3):
        """
        Initialize daily upload scheduler
        Args:
            daily_buffer: DailyBuffer instance
            local_file_writer: LocalFileWriter instance
            s3_uploader: S3Uploader instance
            upload_time: Time to upload (HH:MM format, default midnight)
            file_generation_interval_minutes: How often to generate files (in minutes, default 3)
        """
        self.daily_buffer = daily_buffer
        self.local_file_writer = local_file_writer
        self.s3_uploader = s3_uploader
        self.upload_time = upload_time
        self.file_generation_interval_minutes = file_generation_interval_minutes
        self.running = False
        self.upload_thread = None
        self.file_generation_thread = None
    
    def _parse_upload_time(self) -> tuple:
        """Parse upload time string into hours and minutes"""
        try:
            hour, minute = map(int, self.upload_time.split(':'))
            return hour, minute
        except:
            logger.warning(f"Invalid upload time format: {self.upload_time}, using 00:00")
            return 0, 0
    
    def _get_next_upload_time(self) -> datetime:
        """Calculate next upload time"""
        hour, minute = self._parse_upload_time()
        now = datetime.now()
        next_upload = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # If time has passed today, schedule for tomorrow
        if next_upload <= now:
            next_upload += timedelta(days=1)
        
        return next_upload
    
    def _generate_files_for_date(self, target_date: str) -> Dict[str, str]:
        """
        Generate JSON files for a specific date
        Args:
            target_date: Date in ISO format (YYYY-MM-DD)
        Returns:
            Dictionary mapping patient_id to file_path
        """
        patients = self.daily_buffer.get_patients_for_date(target_date)
        if not patients:
            logger.debug(f"No patients with data for {target_date}")
            return {}
        
        patient_file_paths = {}
        for patient_id in patients:
            events = self.daily_buffer.get_events_for_patient_date(patient_id, target_date)
            if events:
                # Write to local file (will append/update if file exists)
                file_path = self.local_file_writer.write_patient_data(target_date, patient_id, events)
                if file_path:
                    patient_file_paths[patient_id] = file_path
        
        return patient_file_paths
    
    def _upload_yesterday_data(self):
        """Upload data from yesterday: write locally first, then upload to S3"""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        logger.info(f"Starting daily upload for date: {yesterday}")
        
        # Step 1: Generate/update JSON files locally (in case they weren't generated during the day)
        patient_file_paths = self._generate_files_for_date(yesterday)
        
        if not patient_file_paths:
            logger.info(f"No files to upload for {yesterday}")
            return
        
        # Step 2: Upload local files to S3
        logger.info(f"Uploading {len(patient_file_paths)} local files to S3 for {yesterday}")
        success_count = self.s3_uploader.upload_daily_files(yesterday, patient_file_paths)
        logger.info(f"Uploaded {success_count}/{len(patient_file_paths)} files to S3 for {yesterday}")
        
        # Step 3: Clear buffer after successful upload
        if success_count > 0:
            patients = self.daily_buffer.get_patients_for_date(yesterday)
            for patient_id in patients:
                self.daily_buffer.clear_patient_date(patient_id, yesterday)
    
    def _file_generation_loop(self):
        """Periodically generate JSON files throughout the day"""
        while self.running:
            try:
                # Generate files for today's data
                today = date.today().isoformat()
                patient_file_paths = self._generate_files_for_date(today)
                
                if patient_file_paths:
                    logger.info(f"Generated/updated {len(patient_file_paths)} JSON files for {today}")
                else:
                    logger.debug(f"No data to generate files for {today}")
                
                # Wait for next generation interval (convert minutes to seconds)
                time.sleep(self.file_generation_interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Error in file generation loop: {e}", exc_info=True)
                time.sleep(60)  # Wait 1 minute before retrying
    
    def _upload_loop(self):
        """Main upload loop"""
        while self.running:
            try:
                next_upload = self._get_next_upload_time()
                wait_seconds = (next_upload - datetime.now()).total_seconds()
                
                logger.info(f"Next upload scheduled for {next_upload} (in {wait_seconds:.0f} seconds)")
                
                # Wait until upload time
                time.sleep(wait_seconds)
                
                if self.running:
                    self._upload_yesterday_data()
                    
            except Exception as e:
                logger.error(f"Error in upload scheduler: {e}", exc_info=True)
                time.sleep(60)  # Wait 1 minute before retrying
    
    def start(self):
        """Start the upload scheduler and file generation"""
        if self.running:
            return
        
        self.running = True
        
        # Start upload thread
        self.upload_thread = threading.Thread(target=self._upload_loop, daemon=True)
        self.upload_thread.start()
        
        # Start file generation thread
        self.file_generation_thread = threading.Thread(target=self._file_generation_loop, daemon=True)
        self.file_generation_thread.start()
        
        logger.info(
            f"Daily upload scheduler started (upload at {self.upload_time}, "
            f"file generation every {self.file_generation_interval_minutes} minute(s))"
        )
    
    def stop(self):
        """Stop the upload scheduler"""
        self.running = False
        if self.upload_thread:
            self.upload_thread.join(timeout=5)
        logger.info("Daily upload scheduler stopped")
    
    def trigger_manual_upload(self, target_date: str = None):
        """
        Manually trigger upload for a specific date (or yesterday if not specified)
        Args:
            target_date: Date to upload (YYYY-MM-DD format), defaults to yesterday
        """
        if target_date is None:
            target_date = (date.today() - timedelta(days=1)).isoformat()
        
        logger.info(f"Manual upload triggered for date: {target_date}")
        
        # Step 1: Generate/update JSON files locally
        patient_file_paths = self._generate_files_for_date(target_date)
        
        if not patient_file_paths:
            logger.info(f"No files to upload for {target_date}")
            return
        
        # Step 2: Upload local files to S3
        logger.info(f"Uploading {len(patient_file_paths)} local files to S3 for {target_date}")
        success_count = self.s3_uploader.upload_daily_files(target_date, patient_file_paths)
        logger.info(f"Manual upload completed: {success_count}/{len(patient_file_paths)} files uploaded")
        
        # Step 3: Clear buffer after successful upload
        if success_count > 0:
            patients = self.daily_buffer.get_patients_for_date(target_date)
            for patient_id in patients:
                self.daily_buffer.clear_patient_date(patient_id, target_date)

