# frozen-string-literal: true

module Chronos
  module Jobs
    class Monitor < Citrine::Runner::Job::Periodic
      def self.signature_keys
        @signature_keys ||= super.concat([:trace_type])
      end

      attr_reader :latencies
      attr_reader :abnormal_latencies

      def default_operation
        :inspect_latencies
      end

      def any_abnormal_latencies?
        !abnormal_latencies.empty?
      end

      protected

      def set_default_options
        @default_options ||= super.merge!(
          every: "5m",
          abnormal_latency: "10m"
        )
      end

      def set_default_values
        super
        init_abnormal_latency
        init_chronos_trace_logs
      end

      def init_abnormal_latency
        options[:abnormal_latency] = parse_time_interval(options[:abnormal_latency])
      end

      def init_chronos_trace_logs
        options[:chronos_trace_logs] = Chronos::Migration.chronos_trace_logs
      end

      def validate
        super
        if options[:abnormal_latency].nil?
          raise ArgumentError, "Abnormal latency is NOT specified"
        end
      end

      def reset_states
        super
        reset_latencies
        reset_abnormal_latencies
      end

      def reset_latencies
        @latencies = {}
      end

      def reset_abnormal_latencies
        @abnormal_latencies = {}
      end

      def update_states
        super
        update_latencies
        update_abnormal_latencies
      end

      %i[latencies abnormal_latencies].each do |state|
        define_method("update_#{state}") do
          send(state).merge!(result.send(state))
        end
      end

      def update_summary_for_inspect_latencies
        if any_abnormal_latencies?
          @summary += "Found abnormal #{options[:trace_type]} latencies " \
            "(> #{seconds_in_words(options[:abnormal_latency])}):\n" \
            "  Trace From: Latency [Last Synced At] (Job ID)\n"
          abnormal_latencies.each_value do |v|
            @summary += "  #{options[:trace_from]}.#{v[:target]}: " \
              "#{seconds_in_words(v[:latency])} " \
              "[#{v[:last_synced_at]}] (#{v[:job_id]})\n"
          end
        else
          @summary += "No abnormal #{options[:trace_type]} latencies are found."
        end
      end
    end
  end
end
