// NestJS integration for Taskito. Optional — import from `taskito/contrib/nest`;
// requires `@nestjs/common` (and `reflect-metadata`) as peers.
//
//   @Module({ imports: [TaskitoModule.forRoot(queue)] })
//   export class AppModule {}
//
//   // then inject anywhere:
//   constructor(private readonly tasks: TaskitoService) {}

import "reflect-metadata";
import { type DynamicModule, Inject, Injectable, Module } from "@nestjs/common";
import type { Queue } from "../queue";
import type { DeadJob, EnqueueOptions, Job, ResultOptions, Stats } from "../types";

/** DI token for the underlying {@link Queue}. Provided by {@link TaskitoModule.forRoot}. */
export const FLEXIQ_QUEUE = Symbol("FLEXIQ_QUEUE");

/**
 * Injectable wrapper over a Taskito {@link Queue}. Exposes the common producer/inspection
 * methods; reach the full API via {@link TaskitoService.queue}.
 */
@Injectable()
export class TaskitoService {
  constructor(@Inject(FLEXIQ_QUEUE) readonly queue: Queue) {}

  /** Enqueue `task` with positional `args`. Returns the job id. */
  enqueue(task: string, args?: unknown[], options?: EnqueueOptions): string {
    return this.queue.enqueue(task, args ?? [], options);
  }

  /** Await a job's terminal result. */
  result(id: string, options?: ResultOptions): Promise<unknown> {
    return this.queue.result(id, options);
  }

  /** Fetch a job by id, or `null` if unknown. */
  getJob(id: string): Job | null {
    return this.queue.getJob(id);
  }

  /** Aggregate counts across all queues. */
  stats(): Promise<Stats> {
    return this.queue.stats();
  }

  /** Request cooperative cancellation of a job. */
  requestCancel(id: string): boolean {
    return this.queue.requestCancel(id);
  }

  /** List dead-letter entries. */
  deadLetters(limit?: number, offset?: number): Promise<DeadJob[]> {
    return this.queue.deadLetters(limit, offset);
  }
}

/**
 * Dynamic Nest module that provides {@link TaskitoService} bound to a queue. Register it
 * once at the root with {@link TaskitoModule.forRoot}.
 */
@Module({})
// biome-ignore lint/complexity/noStaticOnlyClass: Nest dynamic modules are decorated classes with a static forRoot factory.
export class TaskitoModule {
  /** Provide a {@link TaskitoService} backed by `queue`. */
  static forRoot(queue: Queue): DynamicModule {
    return {
      module: TaskitoModule,
      providers: [{ provide: FLEXIQ_QUEUE, useValue: queue }, TaskitoService],
      exports: [TaskitoService],
    };
  }
}
