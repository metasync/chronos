# frozen-string-literal: true

module Chronos
  module Interactors
    module SimulateOperations
      class PopulateData < Operation
        define_result do
          define_attribute(:dataset) { |ctx| ctx[:dataset] }
        end

        pass :init_context
        step :retrieve_schema
        step :retrieve_foreign_key_samples
        step :generate_data
        step :generate_foreign_key_data
        step :save_data
        step :update_context
        failure :fail_populate_data

        protected

        undef_method :replica

        def init_context(context)
          context[:dataset] = {new: {}, saved: {}, failed: {}}
          context[:foreign_key_samples] = {}
        end

        def retrieve_schema(context)
          context[:schema] = origin(context).schema(job(context)[:from_target])
          context[:primary_key] = get_primary_key(context[:schema])
          !context[:schema].empty?
        end

        def retrieve_foreign_key_samples(context)
          context[:foreign_keys] =
            origin(context).foreign_keys(job(context)[:from_target]).tap do |foreign_keys|
              foreign_keys.each_value do |foreign_key|
                context[:foreign_key_samples][foreign_key.name] =
                  origin(context).run_sql(method(:foreign_key_data),
                    foreign_key: foreign_key,
                    sample_size: job(context)[:limit] / 2 + 1)
              end
            end
          context[:foreign_key_samples].all? { |k, v| !v.empty? }
        end

        def foreign_key_data(db, foreign_key:, sample_size:)
          db.from(foreign_key.table)
            .select(*foreign_key.key)
            .reverse_order(*foreign_key.key)
            .limit(sample_size)
            .all
        end

        def has_foreign_keys?(context)
          !context[:foreign_keys].empty?
        end

        def get_primary_key(schema)
          schema.reduce(nil) do |pk, (name, col)|
            col.primary_key ? name : pk
          end
        end

        def generate_data(context)
          context[:dataset][:new] = (1..job(context)[:limit]).to_a.each_with_object({}) do |i, ds|
            generate_record(context).tap do |record|
              if record[context[:primary_key]].nil?
                ds[i] = record
              else
                ds[record[context[:primary_key]]] = record
              end
            end
          end
        end

        AUDIT_COLUMNS = [:created_at, :updated_at]
        def generate_record(context)
          context[:schema].each_with_object({}) do |(_, column), r|
            generate_column(column).tap do |v|
              r[column.name] = v unless v.nil?
            end
          end
        end

        def generate_column(column)
          if column.primary_key
            fake_ulid if column.type == :string
          elsif AUDIT_COLUMNS.include?(column.name)
            Time.now
          else
            factory_method = "fake_#{column.type}"
            send(factory_method, column) if respond_to?(factory_method, true)
          end
        end

        def fake_ulid
          ULID.generate
        end

        CHARACTERS = ("0".."9").to_a + ("a".."z").to_a + ("A".."Z").to_a
        def fake_string(column)
          Array.new(column.max_length) { CHARACTERS.sample }.join
        end

        def generate_foreign_key_data(context)
          if has_foreign_keys?(context)
            context[:dataset][:new].each_value do |record|
              context[:foreign_keys].each_pair do |name, foreign_key|
                sample = context[:foreign_key_samples][foreign_key.name].sample
                foreign_key.columns.each_with_index do |col, i|
                  record[col] = sample[foreign_key.key[i]]
                end
              end
            end
          else
            true
          end
        end

        def save_data(context)
          origin(context).run_sql(method(:new_data),
            job: job(context),
            dataset: context[:dataset][:new].values)
        end

        def new_data(db, job:, dataset:)
          db.from(job[:from_target]).multi_insert(dataset)
        end

        def update_context(context)
          context[:dataset][:saved].merge!(context[:dataset][:new])
        end

        alias_method :fail_populate_data, :fail_operation_by_task

        def fail_save_data(context)
          context[:dataset][:failed].merge!(context[:dataset][:new])
        end
      end
    end
  end
end
