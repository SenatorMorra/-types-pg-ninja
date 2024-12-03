import pg from "pg";
import defaults from "pg/lib/defaults.js";
import pg_excel from "pg-ninja-excel";

const { Client } = pg;
const excel = new pg_excel();

export default class PG_Ninja {
  #client: pg.Client;
  #log: boolean;
  #send_log: (message: string, color?: "white" | "green" | "yellow" | "red" | "blue") => void;

  constructor(config: pg.ClientConfig = defaults, log: boolean = true) {
    this.#client = new Client(config);
    this.#log = log;
    this.#send_log = (message: string, color: "white" | "green" | "yellow" | "red" | "blue" = "white") => {
      const colors: { [key: string]: string } = {
        white: "\x1b[37m",
        green: "\x1b[32m",
        yellow: "\x1b[33m",
        red: "\x1b[31m",
        blue: "\x1b[34m",
      };

      if (this.#log) {
        console.log(
          colors[color],
          `[${new Date().toLocaleString()}] - ${message}`
        );
      }
    };
    this.#client.connect().then((res) => {
      this.#send_log("successfully connected to the database", "green");
    }).catch((err) => {
      this.#send_log(`Error connecting to the database: ${err.message}`, "red");
    });
  }

  async query(q: string, ...args: any[]): Promise<pg.QueryResult<any>> {
    return new Promise((resolve, reject) => {
      this.#client.query(q, ...args, (err, res) => {
        if (err) {
          this.#send_log(`error with query: ${q}`, "yellow");
          reject(err);
        } else {
          this.#send_log(`success query: ${q}`, "blue");
          if (res.command === "SELECT") {
            (res as any).to_excel = () => {
              excel.pg_to_excel(res.rows);
            };
          }
          resolve(res);
        }
      });
    });
  }

  async transaction(queries: string[], bodies: any[]): Promise<pg.QueryResult<any>> {
    return new Promise(async (resolve, reject) => {
      try {
        const queryArgs = queries.map((x, i) => [x, bodies[i]]);

        await this.#client.query("BEGIN");
        let r: pg.QueryResult<any>;
        for (let i = 0; i < queryArgs.length; i++) {
          r = await this.query(queryArgs[i][0], queryArgs[i][1]);
          if (r.command === undefined) {
            this.#send_log(`failed transaction of ${queries.length} queries`, "yellow");
            await this.#client.query("ROLLBACK");
            reject(new Error("Transaction failed")); // More descriptive error
          }
        }
        await this.#client.query("COMMIT");
        this.#send_log(`success transaction of ${queries.length} queries`, "blue");
        resolve(r);
      } catch (e: any) {
        this.#send_log(`fatal error with transaction of ${queries.length} queries: ${e.message}`, "red");
        await this.#client.query("ROLLBACK"); // Ensure rollback on error
        reject(e);
      }
    });
  }

  async multiquery(qrs: [string, any[]][], ...args: any[]): Promise<{
    completed: number;
    completed_of: number;
    success_query_list: { [key: number]: any };
    failed_query_list: { [key: number]: [string, any[]] };
    error_list: { [key: number]: Error };
    fatal_error: Error | undefined;
    operation_time: number;
    success: boolean;
  }> {
    const params = args[0];
    const saveSuccess = args.at(-1) ?? false;

    const queryArgs = params ? qrs.map((x, i) => [x[0], params[i]]) : qrs;

    const resp = {
      completed: 0,
      completed_of: qrs.length,
      success_query_list: {},
      failed_query_list: {},
      error_list: {},
      fatal_error: undefined,
      operation_time: 0,
      success: true,
    };

    const promises = queryArgs.map(async (queryArg, i) => {
        try {
          const res = await this.query(queryArg[0], queryArg[1]);
          if (!res.rows || res.rows.length === 0) {
            resp.failed_query_list[i] = queryArg;
            resp.error_list[i] = new Error("Query returned no rows");
          } else {
            resp.completed++;
            if (saveSuccess) resp.success_query_list[i] = res.rows;
          }
        } catch (error: any) {
          resp.failed_query_list[i] = queryArg;
          resp.error_list[i] = error;
          resp.success = false;
        }
    });

    return new Promise(async (resolve, reject) => {
        try {
            const startTime = new Date().valueOf();
            await Promise.all(promises);
            resp.operation_time = new Date().valueOf() - startTime;
            this.#send_log(`new ${resp.completed}/${resp.completed_of} multi-query`, "white");
            resolve(resp);
        } catch (e: any) {
          resp.fatal_error = e;
          resp.success = false;
          this.#send_log(`fatal error of ${qrs.length} queries multi-query: ${e.message}`, "red");
          resolve(resp);
        }
    });
  }


  end() {
    this.#client.end();
  }
}
