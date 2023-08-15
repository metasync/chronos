# frozen-string-literal: true

module Chronos
  module Jobs
    class Simulate < Citrine::Runner::Job::Recurring
      include Citrine::Runner::Job::Batchable

      def self.signature_keys
        @signature_keys ||= super.concat([:from_schema, :from_target])
      end

      attr_reader :dataset

      def default_operation
        :populate_data
      end

      protected

      def set_default_options
        @default_options ||= super.merge!(
          batches: 4,
          batch_size: 100,
          wait: "1s"
        )
      end

      def on_init
        super
        init_target
      end

      def init_target
        options[:from_schema], options[:from_target] = options[:from].split(".", 2)
      end

      def validate
        super
        validate_target
      end

      def validate_target
        if options[:from_schema].nil?
          raise ArgumentError, "Schema to simulate MUST be specified - from: schema.target"
        end
        if options[:from_target].nil?
          raise ArgumentError, "Target to simulate MUST be specified - from: schema.target"
        end
      end

      def reset_states
        super
        reset_dataset
      end

      def reset_dataset
        @dataset = {new: {}, saved: {}, failed: {}}
      end

      def update_states
        update_dataset
      end

      def update_dataset
        dataset.each_pair do |k, v|
          result.each { |r| v.merge!(r.dataset[k]) }
        end
      end

      def update_summary_for_populate_data
        if dataset[:saved].size > 0
          @summary += "Populated #{dataset[:saved].size} simulated records " \
            "(#{(dataset[:saved].size / stats[:elapsed_time]).to_i} rps). "
        end

        if dataset[:failed].size > 0
          @summary += "Failed to populate #{dataset[:failed].size} records. "
        end
        @summary += "Wait for #{options[:wait]} secs before next run."
      end
    end
  end
end
