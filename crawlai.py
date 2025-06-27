import asyncio
import json
import re
import os
import glob
import csv
from datetime import datetime
from crawl4ai import AsyncWebCrawler
from pprint import pprint
import time
import pandas as pd


class ProgressTracker:
    def __init__(self, total_urls, progress_file="scrape_progress.txt"):
        self.total_urls = total_urls
        self.completed = 0
        self.successful = 0
        self.failed = 0
        self.start_time = time.time()
        self.progress_file = progress_file
        self.processed_urls = set()

        # Load existing progress if file exists
        self.load_progress()

    def load_progress(self):
        """Load previously processed URLs from progress file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            # Parse the line format: "STATUS|URL"
                            if '|' in line:
                                status, url = line.split('|', 1)
                                self.processed_urls.add(url)
                                self.completed += 1
                                if status == "SUCCESS":
                                    self.successful += 1
                                else:
                                    self.failed += 1
                print(
                    f"📋 Loaded progress: {self.completed} URLs already processed ({self.successful} successful, {self.failed} failed)")
            except Exception as e:
                print(f"⚠️ Error loading progress file: {e}")

    def is_processed(self, url):
        """Check if URL has already been processed"""
        return url in self.processed_urls

    def save_url_progress(self, url, success=True):
        """Save individual URL progress to file"""
        try:
            status = "SUCCESS" if success else "FAILED"
            with open(self.progress_file, 'a', encoding='utf-8') as f:
                f.write(f"{status}|{url}\n")
                f.flush()  # Ensure immediate write to disk
        except Exception as e:
            print(f"⚠️ Error saving progress: {e}")

    def update(self, url, success=True):
        """Update progress and save to file"""
        if not self.is_processed(url):
            self.completed += 1
            if success:
                self.successful += 1
            else:
                self.failed += 1
            self.processed_urls.add(url)
            self.save_url_progress(url, success)

        self.print_progress()

    def print_progress(self):
        elapsed = time.time() - self.start_time
        progress_percent = (self.completed / self.total_urls) * 100

        if self.completed > 0:
            remaining_urls = self.total_urls - self.completed
            if remaining_urls > 0:
                avg_time_per_url = elapsed / self.completed
                estimated_remaining = remaining_urls * avg_time_per_url
                eta_minutes = estimated_remaining / 60
            else:
                eta_minutes = 0
        else:
            eta_minutes = 0

        print(f"Progress: {self.completed}/{self.total_urls} ({progress_percent:.1f}%) | "
              f"✅ {self.successful} | ❌ {self.failed} | "
              f"ETA: {eta_minutes:.1f}m")

        # Add warning if there's a mismatch
        if self.successful + self.failed != self.completed:
            print(
                f"⚠️ WARNING: Progress mismatch detected! Total: {self.completed}, Success: {self.successful}, Failed: {self.failed}")

        # Show current success rate
        if self.completed > 0:
            success_rate = (self.successful / self.completed) * 100
            print(f"📊 Current success rate: {success_rate:.1f}%")

    def get_remaining_urls(self, all_urls):
        """Get list of URLs that haven't been processed yet"""
        return [url for url in all_urls if not self.is_processed(url)]

    def cleanup_progress_file(self):
        """Remove progress file after successful completion"""
        try:
            if os.path.exists(self.progress_file):
                os.remove(self.progress_file)
                print(f"🧹 Cleaned up progress file: {self.progress_file}")
        except Exception as e:
            print(f"⚠️ Error cleaning up progress file: {e}")


class DataPersistenceManager:
    """Manages saving and loading of scraped data with persistence"""

    def __init__(self, base_filename="olx_scraped_data"):
        self.base_filename = base_filename
        self.master_csv = f"{base_filename}_master.csv"
        self.temp_csv = f"{base_filename}_temp.csv"

    def load_existing_data(self):
        """Load existing master CSV data if it exists"""
        if os.path.exists(self.master_csv):
            try:
                df = pd.read_csv(self.master_csv)
                print(f"📂 Loaded {len(df)} existing records from {self.master_csv}")
                return df.to_dict('records')
            except Exception as e:
                print(f"⚠️ Error loading existing data: {e}")
                return []
        return []

    def save_batch_data(self, new_results):
        """Save new batch data to temporary file"""
        if not new_results:
            return False

        try:
            # Save to temporary file first
            df = pd.DataFrame(new_results)
            df.to_csv(self.temp_csv, index=False, encoding='utf-8')
            print(f"💾 Saved batch data to temporary file: {self.temp_csv}")
            return True
        except Exception as e:
            print(f"❌ Error saving batch data: {e}")
            return False

    def merge_and_save_master(self, new_results):
        """Merge new results with existing data and save to master file"""
        try:
            # Load existing data
            existing_data = self.load_existing_data()

            # Create set of existing URLs to avoid duplicates
            existing_urls = set()
            if existing_data:
                existing_urls = {item['url'] for item in existing_data if 'url' in item}

            # Filter out duplicates from new results
            unique_new_results = []
            for result in new_results:
                if result.get('url') not in existing_urls:
                    unique_new_results.append(result)
                else:
                    print(f"🔄 Skipping duplicate URL: {result.get('url')}")

            # Combine all data
            all_data = existing_data + unique_new_results

            if all_data:
                # Save to master file
                df = pd.DataFrame(all_data)
                df.to_csv(self.master_csv, index=False, encoding='utf-8')
                print(f"✅ Master data saved: {len(all_data)} total records ({len(unique_new_results)} new)")

                # Clean up temporary file
                if os.path.exists(self.temp_csv):
                    os.remove(self.temp_csv)

                return True
            else:
                print("⚠️ No data to save")
                return False

        except Exception as e:
            print(f"❌ Error merging and saving master data: {e}")
            return False

    def get_processed_urls(self):
        """Get list of URLs that have been successfully processed"""
        existing_data = self.load_existing_data()
        processed_urls = set()

        for item in existing_data:
            if item.get('scrape_status') == 'success' and item.get('url'):
                processed_urls.add(item['url'])

        return processed_urls


def find_failed_files():
    """Find all failed URL files in the current directory"""
    failed_files = glob.glob("olx_failed_urls_*.json")
    failed_files.sort(key=os.path.getmtime, reverse=True)
    return failed_files


def load_failed_urls(failed_file):
    """Load URLs from a failed URLs file"""
    try:
        with open(failed_file, 'r', encoding='utf-8') as f:
            failed_data = json.load(f)

        urls = []
        if isinstance(failed_data, list):
            for item in failed_data:
                if isinstance(item, str):
                    urls.append(item)
                elif isinstance(item, dict) and 'url' in item:
                    urls.append(item['url'])

        return urls
    except Exception as e:
        print(f"❌ Error loading failed file {failed_file}: {e}")
        return []


def choose_source_file():
    """Let user choose between failed URLs or original JSON file"""
    print("\n🔍 Checking for previous failed URLs...")

    # Check for failed files
    failed_files = find_failed_files()

    if failed_files:
        print(f"\n📁 Found {len(failed_files)} failed URL file(s):")
        for i, file in enumerate(failed_files, 1):
            try:
                # Get file stats
                stat = os.stat(file)
                mod_time = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

                # Count URLs in file
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                url_count = len(data) if isinstance(data, list) else 0

                print(f"  {i}. {file} ({url_count} URLs, modified: {mod_time})")
            except Exception as e:
                print(f"  {i}. {file} (error reading file: {e})")

        print(f"  {len(failed_files) + 1}. Use original JSON file (olx_listing_urls.json)")
        print(f"  0. Exit")

        while True:
            try:
                choice = input(f"\nSelect option (1-{len(failed_files) + 1}, or 0 to exit): ").strip()

                if choice == "0":
                    return None, None
                elif choice == str(len(failed_files) + 1):
                    return "olx_listing_urls.json", "original"
                elif 1 <= int(choice) <= len(failed_files):
                    selected_file = failed_files[int(choice) - 1]
                    return selected_file, "failed"
                else:
                    print(f"❌ Please enter a number between 0 and {len(failed_files) + 1}")
            except ValueError:
                print("❌ Please enter a valid number")
    else:
        print("📄 No failed URL files found. Will use original JSON file.")
        return "olx_listing_urls.json", "original"


# Global variables
progress_tracker = None
data_manager = None


async def fetch_markdown_only(url, crawler):
    """Fetch and return only the markdown content without parsing"""
    global progress_tracker
    try:
        print(f"🔍 Fetching: {url}")
        result = await crawler.arun(url=url)

        # Check if the result is valid and has markdown content
        if not result or not hasattr(result, 'markdown') or not result.markdown.strip():
            error_msg = "No content returned or empty markdown"
            print(f"❌ Error processing {url}: {error_msg}")

            if progress_tracker:
                progress_tracker.update(url, success=False)

            return {
                "url": url,
                "markdown_content": "",
                "scrape_status": "failed",
                "error_message": error_msg,
                "scrape_timestamp": datetime.now().isoformat()
            }

        markdown = result.markdown

        # Basic data structure with just URL and markdown
        data = {
            "url": url,
            "markdown_content": markdown,
            "scrape_status": "success",
            "scrape_timestamp": datetime.now().isoformat(),
            "error_message": ""
        }

        # Update progress tracker for successful fetch
        if progress_tracker:
            progress_tracker.update(url, success=True)

        print(f"✅ Successfully scraped: {url} ({len(markdown)} chars)")
        return data

    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error processing {url}: {error_msg}")

        # Update progress tracker for failed attempt
        if progress_tracker:
            progress_tracker.update(url, success=False)

        return {
            "url": url,
            "markdown_content": "",
            "scrape_status": "failed",
            "error_message": error_msg,
            "scrape_timestamp": datetime.now().isoformat()
        }


async def process_batch(urls, max_concurrent=5, delay_between_requests=1, progress_file="scrape_progress.txt"):
    """Process URLs in batches with rate limiting and progress tracking"""
    global progress_tracker, data_manager

    # Initialize progress tracker
    progress_tracker = ProgressTracker(len(urls), progress_file)

    # Filter out already processed URLs (both from progress file and existing data)
    remaining_urls = progress_tracker.get_remaining_urls(urls)

    # Also filter out URLs that have been successfully scraped before
    if data_manager:
        already_scraped = data_manager.get_processed_urls()
        remaining_urls = [url for url in remaining_urls if url not in already_scraped]
        if len(already_scraped) > 0:
            print(f"📂 Skipping {len(already_scraped)} URLs already in master data")

    if not remaining_urls:
        print("🎉 All URLs have already been processed!")
        return []

    print(f"📋 Found {len(remaining_urls)} URLs remaining to process (out of {len(urls)} total)")

    results = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async with AsyncWebCrawler() as crawler:
        async def process_single_url(url):
            async with semaphore:
                try:
                    result = await fetch_markdown_only(url, crawler)
                    await asyncio.sleep(delay_between_requests)  # Rate limiting
                    return result
                except Exception as e:
                    error_msg = f"Batch processing error: {str(e)}"
                    print(f"❌ Failed to process {url}: {error_msg}")

                    # Ensure progress tracker is updated for batch-level failures too
                    if progress_tracker:
                        progress_tracker.update(url, success=False)

                    return {
                        "url": url,
                        "markdown_content": "",
                        "scrape_status": "failed",
                        "error_message": error_msg,
                        "scrape_timestamp": datetime.now().isoformat()
                    }

        # Create tasks for remaining URLs only
        tasks = [process_single_url(url) for url in remaining_urls]

        # Process all tasks
        print(
            f"🚀 Starting to process {len(remaining_urls)} remaining URLs with max {max_concurrent} concurrent requests...")

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except KeyboardInterrupt:
            print("\n⏸️ Process interrupted by user. Progress has been saved!")
            print(f"📄 Resume by running the script again. Progress saved in: {progress_file}")
            raise

        # Handle any exceptions that were returned
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_result = {
                    "url": remaining_urls[i],
                    "markdown_content": "",
                    "scrape_status": "failed",
                    "error_message": f"Exception during processing: {str(result)}",
                    "scrape_timestamp": datetime.now().isoformat()
                }
                processed_results.append(error_result)

                # Make sure progress tracker counts this as failed
                if progress_tracker:
                    progress_tracker.update(remaining_urls[i], success=False)
            else:
                processed_results.append(result)

        return processed_results


def save_failed_urls_with_retry_count(failed_results, timestamp):
    """Save failed URLs with retry count and better error tracking"""
    if not failed_results:
        return None

    failed_urls_file = f"olx_failed_urls_{timestamp}.json"
    failed_urls_data = []

    # Load existing failed files to track retry count
    existing_failed_files = find_failed_files()
    retry_counts = {}

    for failed_file in existing_failed_files:
        try:
            with open(failed_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    if isinstance(item, dict) and 'url' in item:
                        url = item['url']
                        retry_count = item.get('retry_count', 0)
                        retry_counts[url] = max(retry_counts.get(url, 0), retry_count)
        except:
            pass

    for result in failed_results:
        url = result['url']
        current_retry_count = retry_counts.get(url, 0) + 1

        failed_urls_data.append({
            'url': url,
            'error': result.get('error_message', 'Unknown error'),
            'timestamp': result.get('scrape_timestamp', ''),
            'retry_count': current_retry_count,
            'should_retry': current_retry_count < 3  # Only retry up to 3 times
        })

    with open(failed_urls_file, 'w', encoding='utf-8') as f:
        json.dump(failed_urls_data, f, indent=2, ensure_ascii=False)

    return failed_urls_file


async def main():
    global data_manager

    print("🔄 Enhanced OLX Property Scraper - Persistent Data Collection")
    print("=" * 70)

    # Initialize data persistence manager
    data_manager = DataPersistenceManager()

    # Show existing data status
    existing_data = data_manager.load_existing_data()
    if existing_data:
        print(f"📊 Found {len(existing_data)} existing records in master file")
        successful_existing = len([d for d in existing_data if d.get('scrape_status') == 'success'])
        print(f"✅ {successful_existing} successful, ❌ {len(existing_data) - successful_existing} failed")

    # Let user choose source file
    source_file, file_type = choose_source_file()

    if source_file is None:
        print("👋 Goodbye!")
        return

    progress_file = "scrape_progress.txt"

    if not os.path.exists(source_file):
        print(f"❌ File {source_file} not found!")
        print("Please make sure the file exists in the same directory as this script.")
        return

    try:
        # Load URLs based on file type
        if file_type == "failed":
            print(f"📁 Loading failed URLs from: {source_file}")
            urls = load_failed_urls(source_file)
            if not urls:
                print(f"❌ No valid URLs found in {source_file}")
                return
            print(f"🔄 Loaded {len(urls)} failed URLs for retry")
        else:
            print(f"📁 Loading original URLs from: {source_file}")
            with open(source_file, 'r', encoding='utf-8') as f:
                url_data = json.load(f)

            # Extract URLs - handle different JSON structures
            urls = []
            if isinstance(url_data, list):
                for item in url_data:
                    if isinstance(item, str):
                        urls.append(item)
                    elif isinstance(item, dict) and 'url' in item:
                        urls.append(item['url'])
                    elif isinstance(item, dict) and 'link' in item:
                        urls.append(item['link'])
            elif isinstance(url_data, dict):
                if 'urls' in url_data:
                    urls = url_data['urls']
                elif 'links' in url_data:
                    urls = url_data['links']
                else:
                    for value in url_data.values():
                        if isinstance(value, list):
                            urls.extend(value)

        if not urls:
            print(f"❌ No URLs found in {source_file}")
            print("Please check the structure of your JSON file.")
            return

        print(f"📋 Found {len(urls)} URLs to process")

        # Configure scraping parameters
        print("\n⚙️ Configuration:")
        print("- Max concurrent requests: 5")
        print("- Delay between requests: 1 second")
        print("- Data will be automatically appended to master file")
        print("- Failed URLs will be saved for retry")
        print("- Progress will be tracked in: scrape_progress.txt")

        # Confirm before starting
        response = input("\n🚀 Start scraping? (y/n): ").strip().lower()
        if response != 'y':
            print("👋 Cancelled by user")
            return

        # Start processing
        start_time = time.time()
        print(f"\n🚀 Starting scraping process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        try:
            results = await process_batch(
                urls,
                max_concurrent=5,
                delay_between_requests=1,
                progress_file=progress_file
            )

            # Calculate stats
            total_time = time.time() - start_time
            successful_results = [r for r in results if r.get('scrape_status') == 'success']
            failed_results = [r for r in results if r.get('scrape_status') == 'failed']

            print("\n" + "=" * 70)
            print("📊 SCRAPING BATCH COMPLETE!")
            print("=" * 70)
            print(f"⏱️ Total time: {total_time / 60:.1f} minutes")
            print(f"✅ Successful: {len(successful_results)}")
            print(f"❌ Failed: {len(failed_results)}")
            if len(results) > 0:
                print(f"📊 Success rate: {(len(successful_results) / len(results)) * 100:.1f}%")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save batch data temporarily
            if results:
                data_manager.save_batch_data(results)

            # Merge with existing data and save to master file
            if results:
                if data_manager.merge_and_save_master(results):
                    print("✅ Data successfully merged and saved to master file")
                else:
                    print("⚠️ Warning: Could not merge data properly")

            # Save failed URLs for retry with improved tracking
            if failed_results:
                failed_file = save_failed_urls_with_retry_count(failed_results, timestamp)
                if failed_file:
                    print(f"🔄 Failed URLs saved for retry: {failed_file}")
                    retryable_count = len([f for f in failed_results if f.get('retry_count', 0) < 3])
                    print(f"💡 {retryable_count} URLs can be retried (max 3 attempts)")

            # Show final statistics
            final_data = data_manager.load_existing_data()
            if final_data:
                total_successful = len([d for d in final_data if d.get('scrape_status') == 'success'])
                total_failed = len(final_data) - total_successful
                print(f"\n📈 TOTAL COLLECTION STATUS:")
                print(f"✅ Total successful: {total_successful}")
                print(f"❌ Total failed: {total_failed}")
                print(f"📊 Overall success rate: {(total_successful / len(final_data)) * 100:.1f}%" if len(
                    final_data) > 0 else "N/A")

            # Sample of successful content
            if successful_results:
                print(f"\n📋 Sample of newly scraped content:")
                print("-" * 70)
                for i, result in enumerate(successful_results[:2], 1):
                    print(f"\n{i}. URL: {result['url']}")
                    print(f"   Markdown length: {len(result.get('markdown_content', ''))} characters")
                    print(f"   First 200 chars: {result.get('markdown_content', '')[:200]}...")

            # Clean up progress file on successful completion
            if os.path.exists(progress_file) and not failed_results:
                if progress_tracker:
                    progress_tracker.cleanup_progress_file()

            print(f"\n🎉 Batch completed successfully!")

            # Check if there are still failed URLs to retry
            all_failed_files = find_failed_files()
            if all_failed_files:
                print(f"\n🔄 NEXT STEPS:")
                print(f"💡 Run the script again and select a failed URLs file to retry remaining URLs")
                print(f"📁 Available failed files: {len(all_failed_files)}")
            else:
                print(f"\n🏁 ALL URLS PROCESSED! No failed URLs remaining.")

        except KeyboardInterrupt:
            print(f"\n⏸️ Process interrupted by user.")
            print(f"📄 Progress saved. Run the script again to continue from where you left off.")
            print(f"📁 Progress file: {progress_file}")

            # Still try to save any partial results
            if 'results' in locals() and results:
                data_manager.save_batch_data(results)
                print("💾 Partial results saved to temporary file")

        except Exception as e:
            print(f"\n❌ An error occurred: {e}")
            print(f"📄 Progress has been saved to: {progress_file}")
            print(f"🔄 You can resume by running the script again.")

    except FileNotFoundError:
        print(f"❌ Error: Could not find file {source_file}")
    except json.JSONDecodeError as e:
        print(f"❌ Error reading JSON file: {e}")
        print("Please check that your JSON file is properly formatted.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        print("Please check your Python environment and dependencies.")
        print("Make sure you have pandas installed: pip install pandas")