import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

flowFile = session.get()
if (!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->

    def text = inputStream.getText(StandardCharsets.UTF_8.name())
    def json = new JsonSlurper().parseText(text)

    // --- Helper Transformation Functions ---
    def transformName = {String name ->
        if (name == null){
            return "N/A"
        }else{
            return name.toLowerCase().capitalize()
        }
    }


    // --- Process each item ---
    json.each { item ->
        item.name = transformName(item.name)
        item.abbreviation = item.abbreviation.toUpperCase() ?: "N/A"
        item.base_currency = (item.abbreviation == "Riel") ? true : false
        item.source_system_code = item.source_system_code
    }

    def outputJson = JsonOutput.prettyPrint(JsonOutput.toJson(json))
    outputStream.write(outputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)
