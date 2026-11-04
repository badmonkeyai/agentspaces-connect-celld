// Creates the fleet bucket's container in Azurite (celld needs it to exist): node scripts/container.mjs <name>
import { BlobServiceClient } from "@azure/storage-blob";

const name = process.argv[2] ?? "celld-e2e";
const service = BlobServiceClient.fromConnectionString("UseDevelopmentStorage=true");
const container = service.getContainerClient(name);
const created = await container.createIfNotExists();
console.log(created.succeeded ? `created container ${name}` : `container ${name} exists`);
