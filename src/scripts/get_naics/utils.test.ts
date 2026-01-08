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
import { NaicsResponseType } from "../../schemas/index.ts";
import { TestDbManager } from "../../test-utils/testDBManager.ts";

// Mock the database module
vi.mock("../db", () => ({
  db: {
    insertInto: vi.fn().mockReturnThis(),
    values: vi.fn().mockReturnThis(),
    onConflict: vi.fn().mockReturnThis(),
    execute: vi.fn().mockResolvedValue({}),
    destroy: vi.fn().mockResolvedValue(undefined),
  },
}));

describe("get_naics", () => {
  const API_URL = "https://api.bls.gov/publicAPI/v2/surveys/OEWS/industries/";
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
