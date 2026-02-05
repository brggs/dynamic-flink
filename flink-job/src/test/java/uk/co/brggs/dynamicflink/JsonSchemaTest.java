package uk.co.brggs.dynamicflink;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.AssertionFailedError;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(ErrorCollectingExtension.class)
public class JsonSchemaTest implements ErrorCollectorAware {

    private static Schema schema;

    private static String fileFilter = System.getProperty("schema.files.filter");

    /**
     * Path related to POM file
     */
    private static Path jsonsFolder = new File("src/test/resources/schema").toPath();

    @Getter
    @Setter
    private ErrorCollector errorCollector;

    @BeforeAll
    public static void initSchema() throws IOException {
        // use class loader as this needs to reach main src
        val inputStream = JsonSchemaTest.class.getResourceAsStream("/schema/rule-version-content.json");
        val jsonSchema = new JSONObject(new JSONTokener(inputStream));
        schema = SchemaLoader.load(jsonSchema);
        inputStream.close();
    }

    @Test
    public void validJsonFiles() throws IOException {
        val jsonPaths = getJsonPaths(true);
        assertNotNull(jsonPaths);

        jsonPaths.forEach(
                path -> errorCollector.addErrorIfFails(
                        () -> {
                            try {
                                schema.validate(openJson(path));
                            } catch (ValidationException e) {

                                String message = String.format(
                                        "Test for .%s%s FAILED. Json validation should pass for '%s'.%sReasons:%s%s",
                                        File.separator,
                                        jsonsFolder.relativize(path).toString(),
                                        jsonsFolder.relativize(path).toString(),
                                        System.lineSeparator(),
                                        System.lineSeparator(),
                                        e.toJSON().toString(2)
                                );

                                throw new AssertionFailedError(message, e);
                            }
                            return null;
                        }
                )
        );


    }

    @Test
    public void invalidJsonFiles() throws Throwable {
        val jsonPaths = getJsonPaths(false);
        assertNotNull(jsonPaths);

        jsonPaths.forEach(path ->
                errorCollector.addErrorIfFails(() -> {
                    log.info("File: {}", jsonsFolder.relativize(path).toString());
                    val validationIssue = assertThrows(
                            ValidationException.class,
                            () -> schema.validate(openJson(path)),
                            () -> String.format(
                                    "Test for .%s%s FAILED. Json validation should NOT pass for '%s'",
                                    File.separatorChar,
                                    jsonsFolder.relativize(path).toString(),
                                    jsonsFolder.relativize(path).toString()
                            )
                    );
                    log.info("Reasons: ");
                    log.info(validationIssue.toJSON().toString(2));
                    return null;
                })
        );
    }

    private JSONObject openJson(Path path) {
        try {
            val input = new FileInputStream(path.toFile());
            return new JSONObject(new JSONTokener(input));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Cannot open file");
        }
    }

    private Stream<Path> getJsonPaths(boolean validOnes) throws IOException {
        var subFolder = "valid";
        if (!validOnes) {
            subFolder = "invalid";
        }

        val path = jsonsFolder.resolve(subFolder);
        assertTrue(Files.isDirectory(path));

        if (!Strings.isNullOrEmpty(fileFilter)) {
            log.info("Filter: {}", fileFilter);
        }

        return Files.walk(path)
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().toLowerCase().endsWith(".json"))
                .filter((p) -> fileFilter == null || p.toString().contains(fileFilter));
    }
}
