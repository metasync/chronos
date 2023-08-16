# frozen-string-literal: true

module Chronos
  module Interactors
    module PurgeOperations
      class ArchivalPurge
        protected

        def delete_purgeables(context)
          origin(context).run_sql(method(:archival_purgation),
            job: job(context),
            dataset: context[:dataset][:purgeable].keys,
            trans: context[:trans][:purgeable].keys)
        end

        def archival_purgation(db, job:, dataset:, trans:)
          db.transaction do
            batch_purgation(db, job: job, dataset: dataset)
            db.from(job[:chronos_archive_transactions])
              .where(job[:primary_key] => trans)
              .delete
          end
        end
      end
    end
  end
end
