# frozen-string-literal: true

module Chronos
  module Interactors
    module PurgeOperations
      class Purge
        def delete_purgeables(context)
          raise NotImplementedError.new("#{self.class.name}##{__method__} is an abstract method.")
        end

        protected

        def batch_purgation(db, job:, dataset:)
          purgeable_target(db, job)
            .where(id: dataset)
            .delete
        end

        def update_context(context)
          dataset = context[:dataset]
          trans = context[:trans]
          dataset[:purged].merge!(dataset[:purgeable])
          trans[:purged].merge!(trans[:purgeable])
        end

        def fail_delete_purgeables(context)
          dataset = context[:dataset]
          trans = context[:trans]
          dataset[:failed].merge!(dataset[:purgeable])
          trans[:failed].merge!(trans[:purgeable])
        end
      end
    end
  end
end
