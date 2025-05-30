import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream -> 
    def text = inputStream.getText(StandardCharsets.UTF_8.name())
    def json = new JsonSlurper().parseText(text)

    // --- Helper Transformation Function ---
    def transformName = { String name ->
        if (name == null) {
            return "N/A"
        } else {
            return name.toUpperCase()
        }
    }

    def transformCode = { String code ->
        if (code == null) {
            return "N/A"
        } else {
            return code.toUpperCase()
        }
    }

    def transformParentBranch = { String parentBranch ->
        if (parentBranch == null) {
            return "N/A"
        } else {
            return parentBranch.toUpperCase()
        }
    }

    def transformFullName = { String fullName ->
        if (fullName == null) {
            return null
        } else {
            return fullName
        }
    }

    // --- Process each item ---
    json.each { item ->
        item.name = transformName(item.name)
        item.code = transformCode(item.code)
        item.parent_branch = transformParentBranch(item.parent_branch)
        item.full_name = transformFullName(item.full_name)
        item.province_code = item.province_code ?: "N/A"
        item.source_system_code = item.source_system_code
        item.is_current = 1
    }

    def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(json))
    outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)