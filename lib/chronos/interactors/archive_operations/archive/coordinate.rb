# frozen-string-literal: true

module Chronos
  module Interactors
    module ArchiveOperations
      class Archive
        def open_archive_transactions(context)
          raise NotImplementedError.new("#{self.class.name}##{__method__} is an abstract method.")
        end

        def has_closed_trans_logs?(context)
          !context[:trans_logs][:closed].empty?
        end

        def close_archive_transactions(context)
          if has_closed_trans_logs?(context)
            close_archive_transactions!(context)
          else
            true
          end
        end

        protected

        def close_archive_transactions!(context)
          origin(context).run_sql(method(:complete_archive_transactions),
            job: job(context),
            trans: context[:trans_logs][:closed].keys)
        end

        def complete_archive_transactions(db, job:, trans:)
          db.from(job[:chronos_archive_transactions])
            .where(id: trans).update(closed_at: Time.now)
        end

        def update_trans(context)
          job = job(context)
          dataset = context[:dataset]
          trans = context[:trans]
          trans_logs = context[:trans_logs]

          trans[:closed].merge!(trans_logs[:closed]).each_pair do |id, tx|
            dataset[:archived][tx[job[:target_id]]] =
              dataset[:new][tx[job[:target_id]]] || dataset[:pending][tx[job[:target_id]]]
          end
          trans[:failed].merge!(trans_logs[:failed]).each_pair do |id, tx|
            dataset[:failed][tx[job[:target_id]]] =
              dataset[:new][tx[job[:target_id]]] || dataset[:pending][tx[job[:target_id]]]
          end
        end

        def fail_open_archive_transactions(context)
          trans = context[:trans]
          trans[:failed].merge!(trans[:new])
          trans[:failed].merge!(trans[:pending])
          dataset = context[:dataset]
          dataset[:failed].merge!(dataset[:new])
          dataset[:failed].merge!(dataset[:pending])
        end

        def fail_close_archive_transactions(context)
          trans = context[:trans]
          trans_logs = context[:trans_logs]
          trans[:failed].merge!(trans_logs[:closed])
          trans[:failed].merge!(trans_logs[:failed])
          dataset = context[:dataset]
          dataset[:failed].merge!(dataset[:new])
          dataset[:failed].merge!(dataset[:pending])
        end
      end
    end
  end
end
