package logispin.sportsbook.scheduler;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@EnableSwagger2
public class App  {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
/*    @Bean
    public Docket productApi() {
        return new Docket(DocumentationType.SWAGGER_2).select()
                .apis(RequestHandlerSelectors.basePackage("it.logispin.webhook.rt")).build();
    }*/

    /*@Bean
    public Docket api() {
        ParameterBuilder aParameterBuilder = new ParameterBuilder();
        aParameterBuilder.name("headerName").modelRef(new ModelRef("string")).parameterType("header").required(true).build();


        return new Docket(DocumentationType.SWAGGER_2).select()
                .apis(RequestHandlerSelectors
                        .basePackage("com.novafutur.webhook.realtime.controllers"))
                .paths(PathSelectors.regex("/.*"))
                .build().apiInfo(apiEndPointsInfo());
    }

    private ApiInfo apiEndPointsInfo() {
        return new ApiInfoBuilder().title("NovaFutur WebHook REST API")
                .description("NofaFutur real time notification system")
                .contact(new Contact("Antonio Latela", "www.novafutur.com", "a.latela@novafutur.com"))
                .license("Apache 2.0")
                .licenseUrl("http://www.apache.org/licenses/LICENSE-2.0.html")
                .version("1.0.0")
                .build();
    }*/
}
