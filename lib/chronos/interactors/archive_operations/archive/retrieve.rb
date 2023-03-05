# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class Archive
        %i[retrieve_archivables fail_retrieve_archivable].each do |meth|
          define_method(meth) do |context|
            raise NotImplementedError.new("#{self.class.name}##{__method__} is an abstract method.")
          end
        end

        protected

        def archivable_base(db, job)
          archivable_filter(archivable_target(db, job), job)
        end

        def archivable_filter(dataset, job)
          job[:filter].nil? ? dataset : job[:filter].call(dataset, job)
        end

        def archivable_target(db, job)
          db.from(job[:from_target])
        end
      end
    end
  end
end
