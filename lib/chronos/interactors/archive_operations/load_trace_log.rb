# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class LoadTraceLog < Operation
        define_result do
          define_attribute(:trace_log) { |ctx| ctx[:trace_log] }
        end

        pass :load_trace_log

        alias_method :trace_repository, :replica

        def load_trace_log(context)
          context[:trace_log] = trace_repository(context).run_sql(method(:latest_trace_log),
            job: job(context))
        end

        def target
          :to_target
        end

        protected

        def latest_trace_log(db, job:)
          db.from(:chronos_trace_logs)
            .select(:traced_at, :synced_at)
            .where(job_id: job[:id])
            .reverse_order(:id).first
        end
      end
    end
  end
end
