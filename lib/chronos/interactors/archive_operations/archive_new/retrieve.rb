# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class ArchiveNew
        def retrieve_new_archivables(context)
          context[:dataset][:new] =
            origin(context).run_sql(method(:new_archivable_dataset), job: job(context))
        end
        alias_method :retrieve_archivables, :retrieve_new_archivables

        protected

        def new_archivable_dataset(db, job:)
          new_archivable_base(db, job)
            .order(job[:trace_attr] || job[:primary_key])
            .limit(job[:limit], job[:offset])
            .as_hash(job[:primary_key])
        end

        def new_archivable_base(db, job)
          nab = archivable_base(db, job).exclude(
            job[:primary_key] => created_archive_transactions(db, job)
          )
          if job[:trace_attr]
            nab = nab.where { |o| o.__send__(job[:trace_attr]) >= job[:trace][:traced_at] - job[:trace_drift] }
          end
          nab
        end

        def created_archive_transactions(db, job)
          cat = db.from(job[:chronos_archive_transactions])
            .select(job[:target_id])
            .where(archive_job_id: job[:id])
          if job[:trace_attr]
            cat = cat.where { traced_at >= job[:trace][:traced_at] - job[:trace_drift] }
          end
          cat
        end
      end
    end
  end
end
