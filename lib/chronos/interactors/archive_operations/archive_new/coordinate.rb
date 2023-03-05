# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class ArchiveNew
        def open_archive_transactions(context)
          if has_new_archivables?(context)
            create_new_archive_transactions(context)
            open_archive_transactions!(context)
          else
            true
          end
        end

        protected

        def create_new_archive_transactions(context)
          job = job(context)
          context[:dataset][:new].each_value do |record|
            create_archive_transaction(record, job).tap do |tx|
              context[:trans][:new][tx[:id]] = tx
            end
          end
        end

        def create_archive_transaction(record, job)
          now = Time.now
          {:id => ULID.generate,
           :archive_job_id => job[:id],
           job[:target_id] => record[job[:primary_key]],
           :traced_at => job[:trace_attr] ? record[job[:trace_attr]] : now,
           :opened_at => now}
        end

        def open_archive_transactions!(context)
          origin(context).run_sql(method(:initiate_archive_transactions),
            job: job(context),
            trans: context[:trans][:new].values)
        end

        def initiate_archive_transactions(db, job:, trans:)
          db.from(job[:chronos_archive_transactions]).multi_insert(trans)
        end
      end
    end
  end
end
