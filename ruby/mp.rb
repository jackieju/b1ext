module Mp

    def self.included(base)
        p "include #{base}"
        base.extend(ClassMethods)
        base.send(:include, InstanceMethods)
        
    end
    module ClassMethods
        # @call_set = {}
        def method_added(name)
             @call_set = {} if !@call_set
            p @call_set.inspect
            return if @add == true
            @add = true
            p "method added: #{name}"
            if name.to_s.start_with?("before") || name.to_s.start_with?("after")
                
                
                if @call_set[name] == nil 
                    count = 0
                    @call_set[name] = []
                else
                    count = @call_set[name].size
                end
                nm = "#{name}\##{count}"
                alias_method nm, name
                @call_set[name].push(nm)
                remove_method name
            end
            @add = false
        end
        def call_set
            @call_set
        end

    end
    
    module InstanceMethods
        def method_missing(name, *args, &block) 
            p "missing method #{name}"
             m = self.class.call_set[name.to_sym]
             if m
                 m.each{|mn|
                     _m = method(mn)
                     p "call #{mn} with arg #{args.inspect}"
                     # _m.call(args)
                     
                     self.send(mn, *args)
                     p "called #{mn}"
                }
             else
                 super
             end
        end
    end
    
end