# frozen-string-literal: true

module Chronos
  module Jobs
    class Traceable < Citrine::Runner::Job::CatchUp
      include Citrine::Runner::Job::Batchable

      attr_reader :trace

      def traceable?
        !options[:trace_attr].nil?
      end

      def stale_trace?
        Time.now - trace[:logged_at] >= options[:trace_cycle]
      end

      def to_h
        super.merge!(trace: trace)
      end

      def type
        raise NotImplementedError.new("#{self.class.name}##{__method__} is an abstract method.")
      end

      protected

      def set_default_options
        @default_options ||= super.merge!(
          batches: 4,
          batch_size: 100,
          trace_cycle: "30s",
          wait: "5s"
        )
      end

      def set_default_values
        super
        init_trace
      end

      def init_trace
        @trace = {type: type, current_traced_at: Time.at(0), traced_at: Time.at(0),
                  synced_at: Time.at(0), logged_at: Time.at(0)}
        if options[:trace_cycle]
          options[:trace_cycle] =
            parse_time_interval(options[:trace_cycle])
        end
        if options[:trace_drift]
          options[:trace_drift] =
            parse_time_interval(options[:trace_drift])
        end
      end

      def reset_states
        super
        reset_trace
      end

      def reset_trace
        trace[:started_at] = Time.now
      end

      def post_init
        options[:trace_attr] = options[:trace_attr]&.to_sym
        load_trace_log
        super
      end

      def update_states
        super
        update_trace if traceable?
      end

      %i[traceable_dataset no_more_traces?].each do |name|
        define_method(name) do
          raise NotImplementedError.new("#{self.class.name}##{__method__} is an abstract method.")
        end
      end

      def update_trace
        if success?
          traceable_dataset.each_value do |record|
            if record[options[:trace_attr]] > trace[:current_traced_at]
              trace[:current_traced_at] = record[options[:trace_attr]]
              trace[:traced_at] = trace[:current_traced_at]
            end
          end
        end

        if caught_up?
          trace[:traced_at] = trace[:current_traced_at]
          trace[:synced_at] = no_more_traces? ? trace[:started_at] : trace[:traced_at]
          save_trace_log if stale_trace? && (trace[:traced_at] != Time.at(0))
        end
      end

      def save_trace_log
        r = actor(worker).save_trace_log(job: to_h)
        @result << r
        trace[:logged_at] = Time.now unless r.error?
      end

      def load_trace_log
        r = actor(worker).load_trace_log(job: to_h)
        unless r.trace_log.nil?
          @trace[:current_traced_at] = r.trace_log[:traced_at]
          @trace[:traced_at] = @trace[:current_traced_at]
          @trace[:synced_at] = r.trace_log[:synced_at]
        end
      end
    end
  end
end
