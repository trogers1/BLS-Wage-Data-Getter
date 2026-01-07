import { type TSchema } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";
import { type Static } from "@sinclair/typebox";

export class ValidationError extends Error {
  constructor(
    message: string,
    public readonly errors: string[],
    public readonly data: unknown
  ) {
    super(message);
    this.name = "ValidationError";
  }
}

export function validate<T extends TSchema>(
  schema: T,
  data: unknown,
  context?: string
): Static<typeof schema> {
  const errors = [...Value.Errors(schema, data)];

  if (errors.length > 0) {
    const errorMessages = errors.map(
      (error) =>
        `${error.path}: ${error.message} (${JSON.stringify(error.value)})`
    );

    throw new ValidationError(
      `Validation failed${context ? ` for ${context}` : ""}`,
      errorMessages,
      data
    );
  }

  return data as Static<typeof schema>;
}

export function validateResponse<T extends TSchema>(
  schema: T,
  response: Response,
  data: unknown,
  context?: string
): Static<typeof schema> {
  if (!response.ok) {
    throw new Error(
      `HTTP ${response.status}: ${response.statusText}${context ? ` for ${context}` : ""}`
    );
  }

  return validate(schema, data, context);
}
