import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  vi,
  beforeAll,
  afterAll,
} from "vitest";
import nock from "nock";
import { TimeseriesResponseType } from "../../schemas/index.ts";
import { TestDbManager } from "../../test-utils/testDBManager.ts";

// Mock environment variable
process.env.BLS_API_KEY = "test-api-key";

describe("crawl_wages", () => {
  const API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/";
  let dbManager: TestDbManager;

  beforeAll(async () => {
    dbManager = new TestDbManager();
    await dbManager.start();
  });

  afterAll(async () => {
    await dbManager.stop();
  });

  beforeEach(() => {
    nock.cleanAll();
    vi.clearAllMocks();
  });

  afterEach(() => {
    nock.cleanAll();
  });
});
