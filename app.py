import os
import uuid
from datetime import datetime
import pyodbc
import config
import logging
import logging.handlers as handlers

## Logging Configuration

logger = logging.getLogger("BIFCreationService")
logger.setLevel(logging.DEBUG)

# Maintain log file here
Log = os.path.join(os.path.dirname(os.path.realpath(__file__)), "Log")
if not os.path.exists(Log):
    os.mkdir(Log)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

logHandler = handlers.RotatingFileHandler(
    os.path.join(Log, datetime.now().strftime("%Y_%m_%d.log")),
    maxBytes=5000000,
    backupCount=10,
)
logHandler.setLevel(logging.DEBUG)

# Create a logging format
formatter = logging.Formatter(
    "%(asctime)s: [%(thread)d]:[%(name)s]: %(levelname)s:[BIFCreator] - %(message)s"
)
logHandler.setFormatter(formatter)
ch.setFormatter(formatter)

# Add the handlers to the logger

logger.addHandler(logHandler)
logger.addHandler(ch)


def get_metadata_from_database(file_name):
    logger.debug("Generating metadata for PDF file: {}".format(file_name))
    # Remove the .pdf extension from the filename
    file_name = file_name.replace(".pdf", "")

    try:
        # Establish a connection to the database
        conn = pyodbc.connect(
            r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=.\SQLEXPRESS;DATABASE=SAKRA;UID=sa;PWD=sql@123"
        )

        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # Prepare the SQL query to retrieve metadata
        query = "SELECT [Document Number], [Invoice Number], [Invoice Date], [Vendor Name], [Vendor Code], [Assignment], [Transaction Date], [Amount] FROM [dbo].[Accounts] WHERE [Document Number] = ?"

        # Execute the query and fetch the metadata
        cursor.execute(query, file_name)
        row = cursor.fetchone()

        # Close the cursor and database connection
        cursor.close()
        conn.close()

        # Return the metadata as a dictionary
        metadata = {}
        if row is not None:
            metadata = {
                "Document Number": row[0] if row[0] is not None else "",
                "Invoice No": row[1] if row[1] is not None else "",
                "Invoice Date": row[2] if row[2] is not None else "",
                "Vendor Name": row[3] if row[3] is not None else "",
                "Vendor Code": row[4] if row[4] is not None else "",
                "Assignment": row[5] if row[5] is not None else "",
                "Transaction Date": row[6] if row[6] is not None else "",
                "Amount": row[7] if row[7] is not None else "",
            }

        logger.debug("Metadata generated")
        logger.debug("Metadata: {}".format(metadata))
        return metadata
    except Exception as Error:
        logger.error(Error, exc_info=True)
        return


def main():
    logger.debug("BIF Creation Service Started...")
    logger.debug("Analyzing Folder Structure")

    parent_folder = config.PARENT_FOLDER
    create_location = config.CREATE_LOCATION
    append = config.APPEND
    delete_images = config.DELETE_IMAGES
    create_separate_bif = config.CREATE_SEPARATE_BIF

    for root, dirs, files in os.walk(parent_folder):
        pdf_file_found = False
        account_folder = None
        year = None
        month = None
        sub_category = None

        for file in files:
            if file.endswith(".pdf"):
                pdf_file_found = True
                pdf_file_path = os.path.join(root, file)
                path_parts = root.split(os.sep)
                if len(path_parts) > 2:
                    account_folder = path_parts[-3]
                if len(path_parts) > 1:
                    year = path_parts[-2]
                if len(path_parts) > 0:
                    month = path_parts[-1]
                sub_category = os.path.basename(root)
                break

        if pdf_file_found:
            static_fields = [
                "Document Number",
                "Invoice No",
                "Invoice Date",
                "Vendor Name",
                "Vendor Code",
                "Assignment",
                "Transaction Date",
                "Amount",
            ]

            if create_separate_bif:
                # Logic for creating separate BIF files for each PDF
                doc_count = 1
                metadata = get_metadata_from_database(
                    os.path.splitext(os.path.basename(pdf_file_path))[0]
                )
                
                if metadata is not None:
                    for pdf_file in os.listdir(os.path.dirname(pdf_file_path)):
                        if pdf_file.endswith(".pdf"):
                            bif_filename = os.path.splitext(pdf_file)[0] + ".bif"
                            bif_file_path = os.path.join(os.path.dirname(pdf_file_path), bif_filename)
                            logger.debug("BIF: {} creation started".format(bif_filename))
                            bif_content = "[Documents File]\n[documents]\n"
                            bif_content += "count={}\n".format(doc_count)
                
                            bif_content += 'document{}={}\t"{}"\n'.format(doc_count, sub_category, pdf_file_path)
                
                            bif_content += "doc{}.fields={}\n".format(
                                doc_count, "|".join(static_fields)
                            )
                
                            if metadata is not None:
                                for idx, field in enumerate(static_fields):
                                    value = metadata.get(field, "")
                                    if field in ["Transaction Date", "Invoice Date"]:
                                        value = value.strftime("%m/%d/%Y") if isinstance(value, datetime) else ""
                                    elif isinstance(value, float):
                                        value = str(int(value))  # Convert to an integer
                                    bif_content += "doc{}.field{}={}\n".format(doc_count, idx + 1, value)
                
                            location_path = os.path.relpath(os.path.dirname(pdf_file_path), parent_folder).replace(os.path.sep, "/")
                            bif_content += "doc{}.location={}\n".format(doc_count, location_path)
                            bif_content += "doc{}.create_location={}\n".format(
                                doc_count, str(create_location)
                            )
                            bif_content += "doc{}.append={}\n".format(doc_count, str(append))
                
                            bif_content += "doc{}.delete_images={}\n".format(
                                doc_count, str(delete_images)
                            )
                
                            with open(bif_file_path, "w") as bif_file:
                                bif_file.write(bif_content)
                
                            logger.debug("BIF successfully created.")
                            # doc_count += 1

            else:
                logger.debug(
                    "CREATE SEPARATE BIF is set to FALSE, hence creating a single BIF file for all PDFs within the folder."
                )
                bif_uuid = str(uuid.uuid4())[:8]
                bif_filename = "{}.bif".format(bif_uuid)
                bif_file_path = os.path.join(os.path.dirname(pdf_file_path), bif_filename)
                doc_count = 0
                bif_content = "[Documents File]\n[documents]\n"
                bif_content += "count=0\n"

                for file in os.listdir(os.path.dirname(pdf_file_path)):
                    if file.endswith(".pdf"):
                        file_path = os.path.join(os.path.dirname(pdf_file_path), file)
                        metadata = get_metadata_from_database(
                            os.path.splitext(os.path.basename(file))[0]
                        )
                        doc_count += 1
                        bif_content += 'document{}={}\t"{}"\n'.format(
                            doc_count, sub_category, file_path
                        )
                        bif_content += "doc{}.fields={}\n".format(
                            doc_count, "|".join(static_fields)
                        )

                        if metadata is not None:
                            for idx, field in enumerate(static_fields):
                                value = metadata.get(field, "")
                                if field in ["Transaction Date", "Invoice Date"]:
                                    value = value.strftime(
                                        "%m/%d/%Y"
                                    ) if isinstance(value, datetime) else ""
                                bif_content += "doc{}.field{}={}\n".format(
                                    doc_count, idx + 1, value
                                )

                        location_path = os.path.relpath(os.path.dirname(pdf_file_path), parent_folder).replace(os.path.sep, "/")
                        bif_content += "doc{}.location={}\n".format(doc_count, location_path)
                        bif_content += "doc{}.create_location={}\n".format(
                            doc_count, str(create_location)
                        )
                        bif_content += "doc{}.append={}\n".format(doc_count, str(append))

                        with open(bif_file_path, "a" if append else "w") as bif_file:
                            bif_file.write(bif_content)

                        bif_content += "doc{}.delete_images={}\n".format(
                            doc_count, str(delete_images)
                        )

                bif_content = bif_content.replace("count=0", "count={}".format(doc_count))

                with open(bif_file_path, "w") as bif_file:
                    bif_file.write(bif_content)

                logger.debug("BIF created successfully")

if __name__ == "__main__":
    main()
