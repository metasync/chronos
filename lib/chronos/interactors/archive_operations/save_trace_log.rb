# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class SaveTraceLog < Operation
        define_result

        pass :compose_trace_log
        pass :save_trace_log

        def compose_trace_log(context)
          context[:trace_log] = trace_log(job(context))
        end

        alias_method :trace_repository, :replica

        def save_trace_log(context)
          trace_repository(context).run_sql(method(:create_trace_log),
            job: job(context), trace_log: context[:trace_log])
        end

        def target
          :to_target
        end

        protected

        def create_trace_log(db, job:, trace_log:)
          db.from(job[:chronos_trace_logs]).insert(trace_log)
        end

        def trace_log(job)
          {
            id: ULID.generate,
            job_id: job[:id],
            target: job[target],
            type: job[:trace][:type],
            synced_at: job[:trace][:synced_at],
            traced_at: job[:trace][:traced_at],
            created_at: Time.now
          }
        end
      end
    end
  end
end
