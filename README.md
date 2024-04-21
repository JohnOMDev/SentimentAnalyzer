# Super Sandwiches Review Analysis

Super Sandwiches, a company specializing in preparing delicious sandwiches, has hired our company to analyze user reviews received through various channels. They require a central repository of reviews and the ability to access reviews by date range or retrieve all unprocessed messages. As part of the project team, we have developed several scripts to facilitate this task.

Scripts Overview

ingestion.py

	•	This script accepts one parameter, a file name containing messages to ingest.
	•	The file is in CSV format with headers and has three columns: timestamp, uuid, message.
	•	It checks if the raw_messages table exists. If not, it creates it in the database sup-san-reviews.
	•	The script inserts all messages from the specified file into the raw_messages table.
	•	Existing messages in the table are preserved, ensuring that the table contains both the existing messages and the new ones from the file.

process.py

	•	This script moves data from the raw_messages table to the proc_messages table.
	•	The proc_messages table has the same fields as raw_messages, plus additional fields: category, num_lemm (number of lemmas), and num_char (number of characters).
	•	Categories are assigned based on the content of the messages:
	•	FOOD: Incremented for each lemma related to food (e.g., sandwich, bread, meat).
	•	SERVICE: Incremented for each lemma related to service (e.g., waiter, service) and for entities labeled as MONEY.
	•	GENERAL: Assigned if there are no food or service related lemmas.
	•	Only unprocessed messages are read and inserted into the proc_messages table.
	•	A control table (proc_log) is used to identify messages that need processing. It tracks uuids and processing time.
	•	The flow involves:
	1.	Inserting uuids from raw_messages not present in proc_log.
	2.	Processing messages from raw_messages with empty processing time, calculating new fields, and inserting them into proc_messages.
	3.	Updating the processing time in proc_log for processed messages.

read.py

	•	This script accepts a date as a parameter and generates a JSON file (messages.json) containing processed messages with a timestamp greater than or equal to the specified date.
	•	The JSON schema includes the number of messages and an array of message objects with all fields from the table.

review_card.py

	•	This script crawls KFC reviews from Trustpilot.
	•	It collects reviews and relevant information such as ratings, timestamps, and user comments.
	•	The collected data can be stored in a suitable format for further analysis, such as CSV or JSON.

Repository Structure

	•	scripts: Contains all Python scripts, including ingestion.py, process.py, read.py, and kfc_crawler.py.
	•	data: Placeholder for input CSV files and any crawled data.
	•	README.md: Overview of the task and instructions for running the scripts.

Getting Started

To use the provided scripts:

	1.	Clone this repository to your local machine.
	2.	Ensure you have Python installed along with required dependencies (e.g., pandas, SQLAlchemy).
	3.	Place your input CSV files in the data directory if applicable.
	4.	Run the scripts with appropriate parameters as described in their respective sections.

Feedback

Feedback and suggestions for improving the scripts are welcome. Please feel free to open an issue or submit a pull request with your contributions.

Happy analyzing!
