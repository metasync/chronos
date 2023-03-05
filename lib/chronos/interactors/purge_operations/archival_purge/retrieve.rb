# frozen-string-literal: true

module Chronos
  module Interactors
    module PurgeOperations
      class ArchivalPurge
        def retrieve_purgeables(context)
          job = job(context)
          origin(context).run_sql(method(:archival_purgeables), job: job)
            .each_with_object({}) do |(tx_id, tx), purgeables|
            purgeable = purgeables[tx[job[:target_id]]]
            if purgeable.nil?
              purgeable = purgeables[tx[job[:target_id]]] =
                {closed_at: tx[:closed_at], replicas: {tx_id => tx}}
            else
              if tx[:closed_at] > purgeable[:closed_at]
                purgeable[:closed_at] = tx[:closed_at]
              end
              purgeable[:replicas][tx_id] = tx
            end
            if purgeable[:replicas].size == job[:archive_jobs].size
              context[:dataset][:purgeable][tx[job[:target_id]]] = purgeable
              context[:trans][:purgeable].merge!(purgeable[:replicas])
            end
          end
        end

        protected

        def archival_purgeables(db, job:)
          db.from(job[:chronos_archive_transactions])
            .select(:id, job[:target_id], :closed_at)
            .where(archive_job_id: (job[:archive_jobs].size > 1) ?
                                   job[:archive_jobs] : job[:archive_jobs].first)
            .where { |o| o.closed_at < Time.now - job[:retention_period] }
            .order(:id)
            .limit(job[:limit], job[:offset])
            .as_hash(:id)
        end
      end
    end
  end
end
