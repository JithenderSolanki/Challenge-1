// run the commands
//npm install (It removes all the exceptions and errors)
//tsx runner.ts
import * as fs from "fs"; // File system module for file operations
import * as https from "https"; // HTTP module for downloading files
import * as zlib from "zlib"; // Compression module for decompressing gzip files
import * as tar from "tar"; // Tar module for extracting tar files
import * as csv from "fast-csv"; // CSV module for parsing CSV files
import * as path from "path"; // Path module for working with file paths
import knex from "knex"; // Knex module for interacting with SQLite database
import { DUMP_DOWNLOAD_URL, SQLITE_DB_PATH } from "./resources"; // Constants for download URL and database path

// Helper function to download a file
async function downloadFile(url: string, destination: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    // Create a write stream to save the downloaded file
    const file = fs.createWriteStream(destination);

    // Initiate an HTTP GET request to download the file
    https
      .get(
        url,
        // Specify headers for accepting gzip encoding to handle compressed files
        { headers: { "Accept-Encoding": "gzip, deflate" } },
        (response) => {
          // Retrieve content length to calculate download progress
          const contentLength = parseInt(
            response.headers["content-length"] || "0"
          );
          let downloadedBytes = 0;
          response.pipe(file);

          // Monitor data events to calculate download progress
          response.on("data", (chunk) => {
            downloadedBytes += chunk.length;
            const percent = ((downloadedBytes / contentLength) * 100).toFixed(
              2
            );
            process.stdout.write(`Downloading... ${percent}%\r`);
          });

          // When the download finishes, close the file stream and resolve the promise
          file.on("finish", () => {
            file.close();
            process.stdout.write("\n");
            resolve();
          });
        }
      )
      // Handle errors during download
      .on("error", (error) => {
        fs.unlink(destination, () => reject(error));
      });
  });
}

// Helper function to extract a gzipped tar file
async function extractTarGz(
  source: string,
  destination: string
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    // Create a read stream from the gzipped tar file
    fs.createReadStream(source)
      // Pipe the read stream through a decompression stream
      .pipe(zlib.createGunzip())
      // Handle decompression errors
      .on("error", reject)
      // Pipe the decompressed stream through a tar extraction stream
      .pipe(tar.extract({ cwd: destination, strip: 1 }))
      // Handle tar extraction errors
      .on("error", reject)
      // When the extraction finishes, resolve the promise
      .on("end", resolve);
  });
}

// Helper function to process CSV files and insert data into SQLite database using knex and streams
async function processCSVFiles(directory: string): Promise<void> {
  try {
    // Ensure the 'out' directory exists
    const outDir = path.dirname(SQLITE_DB_PATH);
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    // Initialize SQLite database connection
    console.log("Initializing SQLite database connection");
    const db = knex({
      client: "sqlite3",
      connection: {
        filename: SQLITE_DB_PATH,
      },
      useNullAsDefault: true,
    });

    // Read and parse organizations CSV file
    console.log("Processing organizations CSV");
    const organizationsStream = fs
      .createReadStream(path.join(directory, "organizations.csv"))
      .pipe(csv.parse({ headers: true }));

    const organizations: any[] = [];
    for await (const row of organizationsStream) {
      organizations.push({
        OrganizationId: row["Organization Id"],
        Name: row["Name"],
        Website: row["Website"],
        Country: row["Country"],
        Description: row["Description"],
        Founded: parseInt(row["Founded"]),
        Industry: row["Industry"],
        NumberOfEmployees: parseInt(row["Number of employees"]),
      });
    }

    // Drop and create organizations table
    await db.schema.dropTableIfExists("organizations");
    await db.schema.createTable("organizations", (table) => {
      table.increments("id").primary();
      table.string("OrganizationId").notNullable();
      table.string("Name").notNullable();
      table.string("Website").notNullable();
      table.string("Country").notNullable();
      table.string("Description").notNullable();
      table.integer("Founded").notNullable();
      table.string("Industry").notNullable();
      table.integer("NumberOfEmployees").notNullable();
    });

    // Batch insert organizations data
    await db.batchInsert("organizations", organizations, 100);
    console.log("Organizations processing completed!");
    // Read and parse customers CSV file
    await db.schema.dropTableIfExists("customers");
    console.log("Processing Customers CSV");
    const customersStream = fs
      .createReadStream(path.join(directory, "customers.csv"))
      .pipe(csv.parse({ headers: true }));

    const customers: any[] = [];
    for await (const row of customersStream) {
      customers.push({
        CustomerId: row["Customer Id"],
        FirstName: row["First Name"],
        LastName: row["Last Name"],
        Company: row["Company"],
        City: row["City"],
        Country: row["Country"],
        Phone1: row["Phone 1"],
        Phone2: row["Phone 2"],
        Email: row["Email"],
        Subscription: formatDate(row["Subscription Date"]),
        Website: row["Website"],
      });
    }

    // Drop and create customers table
    await db.schema.dropTableIfExists("customers");
    await db.schema.createTable("customers", (table) => {
      table.increments("id").primary();
      table.string("CustomerId").notNullable();
      table.string("FirstName").notNullable();
      table.string("LastName").notNullable();
      table.string("Company").notNullable();
      table.string("City").notNullable();
      table.string("Country").notNullable();
      table.string("Phone1").notNullable();
      table.string("Phone2").notNullable();
      table.string("Email").notNullable();
      table.text("Subscription").notNullable();
      table.string("Website").notNullable();
    });

    // Batch insert customers data
    await db.batchInsert("customers", customers, 100);
    console.log("Customers processing completed!");
  } catch (error) {
    throw error;
  }
}

// Helper function to parse and format date from DD-MM-YYYY to YYYY-MM-DD
function formatDate(dateString: string): string {
  const [day, month, year] = dateString.split("-");
  return `${year}-${month}-${day}`;
}

// Main function to process data dump
export async function processDataDump(): Promise<void> {
  try {
    // Create the 'tmp' folder if it doesn't exist
    const tmpFolder = "tmp";
    if (!fs.existsSync(tmpFolder)) {
      fs.mkdirSync(tmpFolder);
    }

    console.log("Downloading dump file...");
    await downloadFile(DUMP_DOWNLOAD_URL, "tmp/dump.tar.gz");

    console.log("Extracting dump file...");
    await extractTarGz(path.join(tmpFolder, "dump.tar.gz"), tmpFolder);

    console.log("Processing CSV files...");
    await processCSVFiles(tmpFolder);

    console.log("✅ Data processing completed!");
  } catch (error) {
    console.error("❌ Error processing data:", error);
  }
}
