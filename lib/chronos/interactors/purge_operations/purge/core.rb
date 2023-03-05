# frozen-string-literal: true

module Chronos
  module Interactors
    module PurgeOperations
      class Purge < Operation
        define_result do
          define_attribute(:dataset) { |ctx| ctx[:dataset] }
          define_attribute(:trans) { |ctx| ctx[:trans] }
        end

        pass :init_context
        step :retrieve_purgeables
        step :filter_dependent_purgeables
        step :delete_purgeables
        step :update_context
        failure :fail_purge

        protected

        undef_method :replica

        def init_context(context)
          context[:dataset] = {purgeable: {}, failed: {}, purged: {}}
          context[:trans] = {purgeable: {}, failed: {}, purged: {}}
        end

        alias_method :fail_purge, :fail_operation_by_task
      end
    end
  end
end
