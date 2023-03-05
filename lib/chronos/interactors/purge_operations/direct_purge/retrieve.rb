# frozen-string-literal: true

module Chronos
  module Interactors
    module PurgeOperations
      class DirectPurge
        def retrieve_purgeables(context)
          context[:dataset][:purgeable] =
            origin(context).run_sql(method(:direct_purgeables), job: job(context))
        end

        protected

        def direct_purgeables(db, job:)
          purgeable_base(db, job)
            .select(job[:primary_key], job[:purge_attr])
            .where { |o| o.__send__(job[:purge_attr]) < Time.now - job[:retention_period] }
            .order(job[:primary_key])
            .limit(job[:limit], job[:offset])
            .as_hash(job[:primary_key])
        end
      end
    end
  end
end
