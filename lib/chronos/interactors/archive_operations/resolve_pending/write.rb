# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class ResolvePending
        protected

        def init_archive_transaction_logs(context)
          if has_pending_archivables?(context)
            resolve_pending_archive_transactions(context)
          end
          super
        end

        def resolve_pending_archive_transactions(context)
          closed_trans_logs = retrieve_closed_archive_transaction_logs(context)
          context[:trans][:pending].each_pair do |id, tx|
            if closed_trans_logs.has_key?(id)
              context[:trans_logs][:closed][id] = tx.merge(closed_trans_logs[id])
            else
              job = job(context)
              context[:trans_logs][:new][id] = tx.merge(closed_at: Time.now)
              context[:dataset][:new][tx[job[:target_id]]] =
                context[:dataset][:pending][tx[job[:target_id]]]
            end
          end
        end

        def retrieve_closed_archive_transaction_logs(context)
          replica(context).run_sql(method(:closed_archive_transaction_logs),
            job: job(context),
            trans: context[:trans][:pending].keys)
        end

        def closed_archive_transaction_logs(db, job:, trans:)
          db.from(job[:chronos_archive_transaction_logs])
            .where(id: trans).select(:id, :closed_at)
            .as_hash(:id)
        end
      end
    end
  end
end
