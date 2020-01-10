<?php

namespace App\Controller;

use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

/**
 * Class HomeController
 * @package App\Controller
 */
class DefaultController
{
    /**
     * @Route("/")
     *
     * @return Response
     * @throws \Exception
     */
    public function index()
    {
        return new JsonResponse('OK');
    }
}