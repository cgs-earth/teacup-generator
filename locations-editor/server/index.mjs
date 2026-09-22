import { createApp } from "./app.mjs";
import { readConfig } from "./config.mjs";

const app = createApp(readConfig());
app.listen(
  Number(process.env.PORT || 4080),
  process.env.HOST || "0.0.0.0",
  () =>
    console.log(
      `Reservoir editor listening on port ${process.env.PORT || 4080}`,
    ),
);
