def start
        p "hello java this is ruby"
        path = "#{File.dirname(__FILE__)}/**/*.rb"   # in utility.rb
        path = "/var/sa/script/extroot/**/*.rb"
        p "load ruby from path #{path}"
        #require "/var/sa/ruby/modules.rb"
        #require "/var/sa/ruby/mp.rb"
        #require "/var/sa/ruby/boextension.rb"
        load "/var/sa/script/modules.rb"
        load "/var/sa/script/mp.rb"
        require "/var/sa/script/boextension.rb"

        count = 0
         Dir[path].each { |f|
            p "loading #{f}"
            load(f)
             count +=1
         }
         p "preload #{count} files"


end
start
=begin
def p(m, stack=0)
    if stack >0
         begin
            raise Exception.new
        rescue Exception=>e
            if e.backtrace.size >=2 
                stack  += 1
                stack = e.backtrace.size-1 if stack >= e.backtrace.size
                trace = e.backtrace[1..stack].join("\n") 
                m = "#{m}\n#{trace}"
            end
        end
    end
        
#    Rails.logger.debug(m)
end
=end
def warn(m)
    # Rails.logger.warn(m)
    p m
end
def err(m)
    if m.is_a?(Exception)
        m = "!!!Exception:#{m.inspect}:\n#{m.backtrace[0..9].join("\n")}"
    end
#    Rails.logger.error(m)
    p m
end

def loadObject(path, p=nil, base_path = nil)
       path.gsub!(".", "/")
           begin
            # p "require '"+ path+".rb'"
            if base_path 
                if !base_path.end_with?("/")
                    base_path = "#{base_path}/"
                end
                # eval "require '#{base_path}#{path}.rb'"      
            else
                # eval "require '"+ path+".rb'"      
            end
        target_class = ""
            b = path.split('/')
        i = 0
		b.each{|r|
                        next if r == ""
                        # if target_class != ""
           #                 target_class += "::"
                        # end
                    target_class += "::" if i > 0
                        target_class += r[0..0].upcase+r[1..r.size-1]
                i += 1
        
                    }
            p "target_class #{target_class}"
            if p == nil
                # targetObj=eval target_class+'.new()'
                klass = target_class.split("::").inject(Object) {|x,y| x.const_get(y) }
                targetObj = klass.new
            else
                # targetObj=eval target_class+".new(\"#{p}\")"
                klass = target_class.split("::").inject(Object) {|x,y| x.const_get(y) }
                targetObj = klass.new(p)
            end
        
        #    targetObj.set(o) if o
            return targetObj
        rescue Exception=>e
            p "load object '#{path}' failed"
            # err("#{e.inspect}\n#{e.backtrace[0..9].join("\n")}")
            err e
        end
        
        return nil
        
end


def doAction(cls, action)
    if cls == nil || action == nil
        return
    end
    ob = loadObject(cls)
    if ob == nil
        p "error: loadobject failed(#{cls})"
    else
        return ob.send(action.to_sym)
    end
end
