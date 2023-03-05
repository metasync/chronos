# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class Archive < Operation
        define_result do
          define_attribute(:dataset) { |ctx| ctx[:dataset] }
          define_attribute(:trans) { |ctx| ctx[:trans] }
          define_attribute(:trans_logs) { |ctx| ctx[:trans_logs] }

          protected

          def unique_constraint_violation_error
            Sequel::UniqueConstraintViolation
          end

          def set_ignored_errors
            super.concat([unique_constraint_violation_error])
          end
        end

        pass :init_context
        step :retrieve_archivables
        step :open_archive_transactions
        step :save_archivables
        step :update_trans_logs
        step :close_archive_transactions
        step :update_trans
        failure :fail_archive

        def init_context(context)
          context[:dataset] = {new: {}, pending: {}, failed: {}, archived: {}}
          context[:trans] = {new: {}, pending: {}, failed: {}, closed: {}}
          context[:trans_logs] = {new: {}, failed: {}, closed: {}}
        end

        alias_method :fail_archive, :fail_operation_by_task

        def has_new_archivables?(context)
          !context[:dataset][:new].empty?
        end
      end
    end
  end
end
