package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
	"text/template"
)

type Command struct {
	Cmd         string
	Function    string
	Args        []string
	Result      string
	Error       string
	IsModifying bool
	TtlArgIndex string
	IsVariadic  bool
}

type Data struct {
	PackageName       string
	Commands          []Command
	ModifyingCommands []Command
}

func main() {
	var (
		srcPath  string
		tmplFile string
		outFile  string
		pkgName  string
	)
	flag.StringVar(&srcPath, "src", "../core", "path to core package sources")
	flag.StringVar(&tmplFile, "tmpl", "processor.tmpl", "tmpl file")
	flag.StringVar(&outFile, "out", "processor.gen.go", "output file")
	flag.StringVar(&pkgName, "pkg", "controller", "Output package name.")
	flag.Parse()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, srcPath, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	var commands []Command
	for _, pkg := range pkgs {
		if strings.HasSuffix(pkg.Name, "_test") {
			continue
		}
		for _, file := range pkg.Files {
			fmt.Printf("parsing %s\n", file.Name)
			commands = append(commands, getCommands(file)...)
		}
	}

	data := Data{
		PackageName: pkgName,
		Commands:    commands,
	}

	for _, c := range commands {
		if c.IsModifying {
			data.ModifyingCommands = append(data.ModifyingCommands, c)
		}
	}

	tmpl, err := template.ParseFiles(tmplFile)
	if err != nil {
		panic(err)
	}
	out := bytes.NewBuffer(nil)
	err = tmpl.Execute(out, data)
	if err != nil {
		panic(err)
	}

	formatted, err := format.Source(out.Bytes())
	if err != nil {
		ioutil.WriteFile(outFile, out.Bytes(), 0666)
		panic(err)
	}

	err = ioutil.WriteFile(outFile, formatted, 0666)
	if err != nil {
		panic(err)
	}
}

func getCommands(f *ast.File) []Command {
	var commands []Command

	commandRe := regexp.MustCompile("(?i)^//\\s*@command\\s+(\\w+)")
	ttlRe := regexp.MustCompile("(?i)^//\\s*@Ttl\\s+(\\d+)")
	isModifyingRe := regexp.MustCompile("(?i)^//\\s*@modifying")

	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}

		if fn.Doc == nil {
			continue
		}

		if !fn.Name.IsExported() {
			continue
		}

		isModifying := false
		cmd := ""
		ttlArgIndex := ""
		for _, docStr := range fn.Doc.List {
			if isModifyingRe.FindString(docStr.Text) != "" {
				isModifying = true
				continue
			}

			matches := commandRe.FindStringSubmatch(docStr.Text)
			if len(matches) == 2 {
				cmd = matches[1]
				continue
			}

			matches = ttlRe.FindStringSubmatch(docStr.Text)
			if len(matches) == 2 {
				ttlArgIndex = matches[1]
				continue
			}
		}

		if cmd == "" {
			continue
		}

		args, variadic := getArgs(fn.Type.Params.List)
		c := Command{
			Cmd:         cmd,
			Function:    fn.Name.Name,
			Args:        args,
			IsModifying: isModifying,
			TtlArgIndex: ttlArgIndex,
			IsVariadic:  variadic,
		}

		fmt.Printf("\n\n=== %s() is a command %s, variadic: %t\n", fn.Name.Name, cmd, variadic)

		var results []string
		if fn.Type.Results != nil {
			results, _ = getArgs(fn.Type.Results.List)
		}

		switch len(results) {
		case 0:
			//do nothing
		case 1:
			if results[0] == "error" {
				c.Error = results[0]
			} else {
				c.Result = results[0]
			}
		case 2:
			c.Result = results[0]
			c.Error = results[1]
		default:
			log.Fatalf("Invalid return type of %s(): %s", c.Function, results)
		}

		fmt.Printf("Args: %s\n", c.Args)
		fmt.Printf("Result: %s\n", c.Result)
		fmt.Printf("Err: %s\n", c.Error)
		commands = append(commands, c)
	}

	return commands
}

func getArgs(list []*ast.Field) (args []string, isVariadic bool) {
	for _, p := range list {
		for range p.Names { // to correctly process args like as "DKeys(key, patternk string)"

			switch paramType := p.Type.(type) {
			case *ast.Ident:
				args = append(args, paramType.Name)
			case *ast.ArrayType:
				is2d := false
				var EltName string
				if doubleSlice, ok := paramType.Elt.(*ast.ArrayType); ok {
					is2d = true
					EltName = doubleSlice.Elt.(*ast.Ident).Name
				} else {
					EltName = paramType.Elt.(*ast.Ident).Name
				}

				var strType string
				if is2d {
					strType = "[]"
				}
				switch EltName {
				case "string":
					strType += "[]string"
				case "byte":
					strType += "[]byte"
				default:
					log.Fatalf("Unknown Elt type: %v", paramType.Elt.(*ast.Ident).Name)
				}
				if strType == "[]string" || strType == "[][]byte" {
					isVariadic = true
				}

				args = append(args, strType)
				//fmt.Printf("%s\n", strType)
			default:
				log.Fatalf("NEW ARG TYPE: %T\n", p.Type)

			}
		}
	}

	return args, isVariadic
}
