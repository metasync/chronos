# frozen-string-literal: true

module Chronos
  class Operation < Citrine::Operation
    def self.define_result(&blk)
      result_class = const_set(:Result, Class.new(Citrine::Operation::Result, &blk))
      const_set(:Success,
        Class.new(result_class) do
          code Citrine::Operation::Result::DEFAULT_SUCCESS_CODE
          message Citrine::Operation::Result::DEFAULT_SUCCESS_CODE
        end)
      const_set(:Failure,
        Class.new(result_class) do
          code { |ctx| self.class.name.demodulize + "Failure" }
          message do |ctx|
            "Failed to #{ctx[:failed_task].name.to_s.tr("_", " ")}: " \
            "#{ctx[:error].message} (#{ctx[:error].class.name})"
          end
        end)
    end

    protected

    def job(context)
      context[:params][:job]
    end

    def origin(context)
      actor("#{job(context)[:from_schema]}_repository")
    end

    def replica(context)
      actor("#{job(context)[:to_schema]}_repository")
    end

    def fail_operation_by_task(context)
      "fail_#{context[:failed_task].name}".tap do |task|
        send(task, context) if respond_to?(task, true)
      end
      context[:result] = self.class.const_get(:Failure).new(context)
    end
  end
end
