import { processDataDump } from "./challenge";

/**
 * This is the entry point for the challenge.
 * This will run your code.
 */
async function main() {
  await processDataDump();
  // Terminate the program with a success exit code
  process.exit(0);
}

// Run the main function
main();
