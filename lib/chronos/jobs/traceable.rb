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

      def on_init
        super
        init_source_and_target
      end

      def init_source_and_target
        init_source_from
        init_target_to
        options[:target_id] =
          options[:primary_key_uuid] ? :target_uuid : :target_id
        options[:chronos_archive_transactions] =
          Chronos::Migration.chronos_archive_transactions(primary_key_uuid: options[:primary_key_uuid])
        options[:chronos_archive_transaction_logs] =
          Chronos::Migration.chronos_archive_transaction_logs(primary_key_uuid: options[:primary_key_uuid])
        options[:chronos_trace_logs] = Chronos::Migration.chronos_trace_logs
      end

      def init_source_from
        options[:from_repo], options[:from_target] = options[:from].split(".", 2)
        options[:qualified_from_target] = Chronos::Migration.qualified_sequeal_identifier(options[:from_target])
      end

      def init_target_to
        options[:to_repo], options[:to_target] = options[:to].split(".", 2)
        options[:to_target] ||= options[:from_target]
        options[:qualified_to_target] = Chronos::Migration.qualified_identifier(options[:to_target])
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

      def validate
        super
        validate_source_from
        validate_target_to
      end

      def validate_source_from
        if options[:from_repo].nil?
          raise ArgumentError, "Source repository MUST be specified - from: repo.schema.target / from: repo.target"
        end
        if options[:from_target].nil?
          raise ArgumentError, "Source target MUST be specified - from: repo.schema.target / from: repo.target"
        end
      end

      def validate_target_to
        if options[:to_repo].nil?
          raise ArgumentError, "Destination repository MUST be specified - to: repo.schema.target / to: repo.target"
        end
        if options[:to_target].nil?
          raise ArgumentError, "Destination target MUST be specified - to: repo.schema.target / to: repo.target"
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
