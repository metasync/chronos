# frozen-string-literal: true

module Chronos
  module Interactors
    module PurgeOperations
      class DirectPurge
        protected

        def delete_purgeables(context)
          origin(context).run_sql(method(:direct_purgation),
            job: job(context),
            dataset: context[:dataset][:purgeable].keys)
        end

        alias_method :direct_purgation, :batch_purgation
      end
    end
  end
end
