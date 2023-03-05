# frozen-string-literal: true

module Chronos
  module Interactors
    module PurgeOperations
      class Purge
        def retrieve_purgeables(context)
          raise NotImplementedError.new("#{self.class.name}##{__method__} is an abstract method.")
        end

        def filter_dependent_purgeables(context)
          job(context)[:dependents].each_pair do |dependent, foreign_key|
            find_dependent_purgeables(context,
              dependent: dependent,
              foreign_key: foreign_key).each do |id|
              context[:dataset][:purgeable].delete(id).tap do |ds|
                ds[:replicas].each_key do |k|
                  context[:trans][:purgeable].delete(k)
                end
              end
            end
          end
        end

        protected

        def purgeable_base(db, job)
          purgeable_filter(purgeable_target(db, job), job)
        end

        def purgeable_filter(dataset, job)
          job[:filter].nil? ? dataset : job[:filter].call(dataset, job)
        end

        def purgeable_target(db, job)
          db.from(job[:from_target])
        end

        def find_dependent_purgeables(context, **dependent)
          origin(context).run_sql(method(:dependent_purgeables),
            dataset: context[:dataset][:purgeable].keys,
            **dependent)
        end

        def dependent_purgeables(db, dataset:, dependent:, foreign_key:)
          foreign_key = foreign_key.to_sym
          db.from(dependent)
            .select(foreign_key).distinct
            .where(foreign_key => dataset)
            .map(foreign_key)
        end
      end
    end
  end
end
