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
import { SocResponseType } from "../../schemas/index.ts";
import { TestDbManager } from "../../test-utils/testDBManager.ts";

describe("get_socs", () => {
  const API_URL = "https://api.bls.gov/publicAPI/v2/surveys/OEWS/occupations/";
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
