# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class ResolvePending < Archive
        def retrieve_pending_archivables(context)
          context[:dataset][:pending] =
            origin(context).run_sql(method(:pending_archivable_dataset),
              job: job(context))
        end
        alias_method :retrieve_archivables, :retrieve_pending_archivables

        protected

        def pending_archivable_dataset(db, job:)
          pending_archivable_base(db, job)
            # .order(Sequel[job[:from_target].to_sym][job[:primary_key]])
            .limit(job[:limit], job[:offset])
            .as_hash(job[:primary_key])
        end

        def pending_archivable_base(db, job)
          target = Sequel[job[:from_target].to_sym]
          catxs = Sequel[job[:chronos_archive_transactions]]
          archivable_base(db, job)
            .select(target.*)
            .join(job[:chronos_archive_transactions], catxs[job[:target_id]] => target[job[:primary_key]])
            .select_more(catxs[:id].as(:archive_transaction_id),
              catxs[:traced_at].as(:archive_transaction_traced_at),
              catxs[:opened_at].as(:archive_transaction_opened_at))
            .where(catxs[:archive_job_id] => job[:id])
            .where(catxs[:closed_at] => nil)
        end
      end
    end
  end
end
