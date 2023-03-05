# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class ResolvePending
        def has_pending_archivables?(context)
          !context[:dataset][:pending].empty?
        end

        def open_archive_transactions(context)
          if has_pending_archivables?(context)
            create_pending_archive_transactions(context)
          else
            true
          end
        end

        protected

        def create_pending_archive_transactions(context)
          job = job(context)
          context[:dataset][:pending].each_value do |record|
            create_archive_transaction(record, job).tap do |tx|
              context[:trans][:pending][tx[:id]] = tx
            end
            # Clean up original record by removing transaction related columns
            record.delete(:archive_transaction_id)
            record.delete(:archive_transaction_traced_at)
            record.delete(:archive_transaction_opened_at)
          end
        end

        def create_archive_transaction(record, job)
          {:id => record[:archive_transaction_id],
           :archive_job_id => job[:id],
           job[:target_id] => record[job[:primary_key]],
           :traced_at => record[:archive_transaction_traced_at],
           :opened_at => record[:archive_transaction_opened_at]}
        end
      end
    end
  end
end
