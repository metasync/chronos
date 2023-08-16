# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class Archive
        def save_archivables(context)
          init_archive_transaction_logs(context)
          if has_new_archivables?(context)
            transform_archivables(context)
            save_archivables!(context)
          end
          true
        end

        protected

        def init_archive_transaction_logs(context)
          if has_new_archivables?(context)
            init_new_archive_transaction_logs(context)
          end
        end

        def init_new_archive_transaction_logs(context)
          context[:trans][:new].each_pair do |id, tx|
            context[:trans_logs][:new][id] = tx.merge(closed_at: Time.now)
          end
        end

        def transform_archivables(context)
          transform_attributes(context)
        end

        def transform_attributes(context)
          job = job(context)
          context[:dataset][:new].each_value do |record|
            unless job[:reject_attrs].empty?
              reject_attributes(record, job[:reject_attrs])
            end
            unless job[:select_attrs].empty?
              select_attributes(record, job[:select_attrs])
            end
            unless job[:rename_attrs].empty?
              rename_attributes(record, job[:rename_attrs])
            end
            job[:transform]&.call(record, job)
          end
        end

        def reject_attributes(record, reject_attrs)
          record.delete_if { |k, v| reject_attrs.include?(k) }
        end

        def select_attributes(record, select_attrs)
          record.keep_if { |k, v| select_attrs.include?(k) }
        end

        def rename_attributes(record, rename_attrs)
          rename_attrs.each_pair do |origin_attr, renamed_attr|
            if record.has_key?(origin_attr)
              record[renamed_attr] = record.delete(origin_attr)
            end
          end
        end

        def save_archivables!(context)
          replica(context).run_sql(method(:new_archivables),
            job: job(context),
            dataset: context[:dataset][:new].values,
            trans_logs: context[:trans_logs][:new].values)
        end

        def new_archivables(db, job:, dataset:, trans_logs:)
          db.transaction do
            db.from(job[:qualified_to_target]).multi_insert(dataset)
            db.from(job[:chronos_archive_transaction_logs]).multi_insert(trans_logs)
          end
        end

        def update_trans_logs(context)
          trans_logs = context[:trans_logs]
          trans_logs[:closed].merge!(trans_logs[:new])
        end

        def fail_save_archivables(context)
          trans = context[:trans]
          trans_logs = context[:trans_logs]
          if trans_logs[:new].empty?
            trans_logs[:failed].merge!(trans[:pending])
          else
            trans_logs[:failed].merge!(trans_logs[:new])
          end
        end
      end
    end
  end
end
