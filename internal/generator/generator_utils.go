// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package generator is a utility package that is not linked against directly by
// the API code and is instead used as common functionality for calls to `go generate`
package generator

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

/*
 * Generator utils provides helper functions for generating blocks of go code from C enums
 */

const copyrightHeader = `// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

`

const enumBlockRegexTemplate string = "  typedef enum %s\n  {\n((.*\n?)*)\n  } %s_t;"
const enumEntryRegexTemplate string = "    %s_([A-Z0-9_]+) += +(-?[0-9]+),? +/\\*\\*< ?([^\\*]+) *\\*/"
const defineRegexTemplate string = "#define %s_([A-Z0-9_]+) +\\(?\"([A-Z0-9_]+)\"\\)? +/\\*\\*< ?(.+)(\n *\\*?[^\\*]*)*\n? *\\*/"

// FromDefine function
func FromDefine(inputFile, outputFile, enumPrefix, variablePrefix, header, footer string, byReference bool) {
	data := readFile(inputFile)

	defineRegexExpr := fmt.Sprintf(defineRegexTemplate, enumPrefix)
	entriesRegex, err := regexp.Compile(defineRegexExpr)
	if err != nil {
		// regex error
		fmt.Println(err)
		os.Exit(1)
	}
	entries := entriesRegex.FindAllSubmatch(data, -1)
	if len(entries) == 0 {
		// no subcodes matching entriesRegex
		fmt.Printf("Could not find any entries in definition block using enum entry regex: '%s'\n", defineRegexExpr)
		os.Exit(1)
	}

	generatedCode := &bytes.Buffer{}
	generatedCode.WriteString(copyrightHeader)
	generatedCode.WriteString(header)
	for _, row := range entries {
		name := fmt.Sprintf("%s%s", variablePrefix, upperSnakeCaseToPascalCase(string(row[1])))
		value := string(row[2])
		var valueString string
		if byReference {
			valueString = fmt.Sprintf("C.%s_%s", enumPrefix, string(row[1]))
		} else {
			valueString = fmt.Sprintf("\"%s\"", value)
		}
		comment := strings.ReplaceAll(string(row[3]), "\n", "")
		line := fmt.Sprintf("	// %s: %s\n	%s = %s\n", name, comment, name, valueString)
		generatedCode.WriteString(line)
	}
	generatedCode.WriteString(footer)

	writeFile(outputFile, generatedCode)
}

// FromEnum function
func FromEnum(inputFile, outputFile, enumName, enumPrefix, variablePrefix, typeName, header, footer string, byReference bool) {
	data := readFile(inputFile)

	enumBlockRegexExpr := fmt.Sprintf(enumBlockRegexTemplate, enumName, enumName)
	// get relevant section of solClient.h
	enumBlockRegex, err := regexp.Compile(enumBlockRegexExpr)
	if err != nil {
		// regex error
		fmt.Println(err)
		os.Exit(1)
	}

	enumBlock := enumBlockRegex.Find(data)
	if len(enumBlock) == 0 {
		// no data
		fmt.Printf("Could not find enum block definition with name '%s'\n ", enumName)
		os.Exit(1)
	}

	enumEntryRegexExpr := fmt.Sprintf(enumEntryRegexTemplate, enumPrefix)
	// get each entry, supporting multi line comments, capturing relevant groups
	entriesRegex, err := regexp.Compile(enumEntryRegexExpr)
	if err != nil {
		// regex error
		fmt.Println(err)
		os.Exit(1)
	}

	entries := entriesRegex.FindAllSubmatch(enumBlock, -1)
	if len(entries) == 0 {
		// no subcodes matching entriesRegex
		fmt.Printf("Could not find any entries in definition block using enum entry regex: '%s'\n", enumEntryRegexExpr)
		os.Exit(1)
	}

	// parse each entry, converting names and comments
	generatedCode := &bytes.Buffer{}
	generatedCode.WriteString(copyrightHeader)
	generatedCode.WriteString(header)
	for _, row := range entries {
		name := fmt.Sprintf("%s%s", variablePrefix, upperSnakeCaseToPascalCase(string(row[1])))
		value := string(row[2])
		var valueString string
		if byReference {
			valueString = fmt.Sprintf("C.%s_%s", enumPrefix, string(row[1]))
		} else {
			valueString = value
		}
		comment := strings.ReplaceAll(string(row[3]), "\n", "")
		line := fmt.Sprintf("	// %s: %s\n	%s %s = %s\n", name, comment, name, typeName, valueString)
		generatedCode.WriteString(line)
	}
	generatedCode.WriteString(footer)

	writeFile(outputFile, generatedCode)
}

func readFile(inputFile string) []byte {
	data, err := ioutil.ReadFile(inputFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return data
}

func writeFile(outputFile string, generatedCode *bytes.Buffer) {
	// Use built in go formatter to format the codes
	formattedCode, err := format.Source(generatedCode.Bytes())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Output the formatted code ot the output file, overwriting any content
	generatedFile, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = generatedFile.Truncate(0)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer generatedFile.Close()
	generatedFile.Write(formattedCode)
}

func upperSnakeCaseToPascalCase(upperSnakeCase string) string {
	words := strings.Split(upperSnakeCase, "_")
	result := ""
	initialisms := []string{"ID", "TTL", "TCP", "IP"}
wordLoop:
	for _, word := range words {
		for _, initial := range initialisms {
			if word == initial {
				result += word
				continue wordLoop
			}
		}
		result += string(word[0]) + strings.ToLower(word[1:])
	}
	return result
}
